package sangria.execution

import org.scalatest.{Matchers, WordSpec}
import sangria.execution.deferred.{Deferred, DeferredResolver}
import sangria.parser.QueryParser
import sangria.schema._
import sangria.util.FutureResultSupport

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Success, Try}

class ProjectorSpec extends WordSpec with Matchers with FutureResultSupport {
  case class Product(id: String, variants: List[Variant])
  case class Variant(id: String, relatedProductIds: List[String], categories: List[Reference])

  case class ProductDefer(productIds: List[String]) extends Deferred[List[Right[String, Product]]]

  case class Category(id: String, name: String)
  case class Reference(id: String, typeId: String)
  case class CategoryReferenceDefer(references: List[Reference]) extends Deferred[List[Category]]

  val CategoryType = ObjectType("Category",
    fields[Unit, Category](
      Field("id", IDType,
        tags = ProjectionName("id") :: Nil,
        resolve = _.value.id),
      Field("name", StringType,
        tags = ProjectionName("name") :: Nil,
        resolve = _.value.name)
    ))

  val VariantType = ObjectType("Variant", () ⇒ fields[Ctx, Variant](
    Field("id", IDType, resolve = _.value.id),
    Field("mixed", StringType,
      tags = ProjectionName("mixed1") :: ProjectionName("mixed2") :: Nil,
      resolve = _.value.id),
    Field("typeId", StringType, tags = ProjectionExclude :: Nil, resolve = _ ⇒ "variant"),
    Field("relatedProducts", ListType(ProductType),
      tags = ProjectionName("rp") :: Nil,
      resolve = Projector(1, (ctx, projected) ⇒ projected match {
        case Vector(ProjectedName("id", _)) ⇒ Value(ctx.value.relatedProductIds map (Left(_)))
        case _ ⇒ ProductDefer(ctx.value.relatedProductIds)
      })),
    Field("categories", ListType(CategoryType),
      tags = ProjectionName("categories.id") :: ProjectionName("categories.typeId") :: Nil,
      resolve = Projector { (ctx, projected) ⇒
        val references = ctx.value.categories
        ctx.ctx.categoryProjections = projected
        CategoryReferenceDefer(references)
      })
  ))

  val ProductType: ObjectType[Unit, Either[String, Product]] =
    ObjectType("Product", List[Field[Unit, Either[String, Product]]](
      Field("id", IDType, resolve = _.value.fold(identity, _.id)),
      Field("variantIds", ListType(IDType),
        tags = ProjectionName("masterVariant.id") :: ProjectionName("variants.id") :: Nil,
        resolve = _ ⇒ Nil),
      Field("typeId", StringType, tags = ProjectionExclude :: Nil, resolve = _ ⇒ "product"),
      Field("masterVariant", VariantType,
        tags = ProjectionName("master1") :: ProjectionName("master2") :: Nil,
        resolve = _.value.right.get.variants.head),
      Field("variants", ListType(VariantType), resolve = _.value.right.get.variants.tail)
    ))

  val QueryType = ObjectType("Query", fields[Ctx, Unit](
    Field("products", ListType(ProductType), resolve = _.ctx.products map (Right(_))),
    Field("projectAll", ListType(ProductType), resolve = Projector((ctx, proj) ⇒ {
      ctx.ctx.allProjections = proj
      ctx.ctx.products map (Right(_))
    })),
    Field("projectOne", ListType(ProductType), resolve = Projector(1, (ctx, proj) ⇒ {
      ctx.ctx.oneLevelprojections = proj
      ctx.ctx.products map (Right(_))
    }))
  ))

  val schema = Schema(QueryType)

  trait WithProducts {
    def products: List[Product]
  }

  trait WithCategories {
    def categories: List[Category]
  }

  class Ctx extends WithProducts with WithCategories {
    val products: List[Product] = List(
      Product("1", List(
        Variant("1", Nil, List(Reference("1", "categories"), Reference("2", "categories"))),
        Variant("2", List("1", "2"), Nil)
      )),
      Product("2", List(
        Variant("1", Nil, Nil)
      ))
    )

    val categories: List[Category] = List(
      Category("1", "category 1"),
      Category("2", "category 2")
    )

    var allProjections: Vector[ProjectedName] = Vector.empty
    var categoryProjections: Vector[ProjectedName] = Vector.empty
    var oneLevelprojections: Vector[ProjectedName] = Vector.empty
  }

  class ProductResolver extends DeferredResolver[WithProducts] {
    override def resolve(deferred: Vector[Deferred[Any]], ctx: WithProducts, queryState: Any)(implicit ec: ExecutionContext) = deferred map {
      case ProductDefer(ids) ⇒
        Future.fromTry(Try(ids map (id ⇒ Right(ctx.products.find(_.id == id).get))))
    }
  }

  class CategoryResolver extends DeferredResolver[WithCategories] {
    override def resolve(deferred: Vector[Deferred[Any]], ctx: WithCategories, queryState: Any)(implicit ec: ExecutionContext) = deferred map {
      case CategoryReferenceDefer(references) ⇒
        Future.fromTry(Try(references map (ref ⇒ ctx.categories.find(_.id == ref.id).get)))
    }
  }

  "Projector" should {
    "project all fields except explicitly marked with `NoProjection`" in {
      val Success(query) = QueryParser.parse(
        """
          {
            projectAll {
              id
              typeId
              variants {
                id
                typeId
                relatedProducts {
                  id
                  typeId
                  variants {
                    id
                  }
                }
              }
            }
            projectOne {
              id
              typeId
              variants {
                id
                typeId
              }
            }
          }
        """)

      val ctx = new Ctx

      Executor.execute(schema, query, ctx, deferredResolver = new ProductResolver).await should be (
        Map("data" →
          Map(
            "projectAll" →
              List(
                Map(
                  "id" → "1",
                  "typeId" → "product",
                  "variants" → List(
                    Map(
                      "id" → "2",
                      "typeId" → "variant",
                      "relatedProducts" → List(
                        Map(
                          "id" → "1",
                          "typeId" → "product",
                          "variants" → List(
                            Map("id" → "2"))),
                        Map(
                          "id" → "2",
                          "typeId" → "product",
                          "variants" → Nil))))),
                Map(
                  "id" → "2",
                  "typeId" → "product",
                  "variants" → Nil)),
          "projectOne" →
            List(
              Map(
                "id" → "1",
                "typeId" → "product",
                "variants" → List(
                  Map(
                    "id" → "2",
                    "typeId" → "variant"))),
              Map(
                "id" → "2",
                "typeId" → "product",
                "variants" → Nil)))))

      ctx.allProjections should be (
        Vector(
          ProjectedName("id", Vector.empty),
          ProjectedName("variants", Vector(
            ProjectedName("id", Vector.empty),
            ProjectedName("rp", Vector(
              ProjectedName("id", Vector.empty),
              ProjectedName("variants", Vector(
                ProjectedName("id", Vector.empty)))))))))

      ctx.oneLevelprojections should be (
        Vector(
          ProjectedName("id", Vector.empty),
          ProjectedName("variants", Vector.empty)))
    }

    "handle multiple projected names" in {
      val Success(query) = QueryParser.parse(
        """
          {
            projectAll {
              id
              variantIds
              masterVariant {
                mixed
              }
              variants {
                id
                mixed
              }
            }

            projectOne {
              id
              variantIds
              masterVariant {
                mixed
              }
              variants {
                id
                mixed
              }
            }
          }
        """)

      val ctx = new Ctx

      Executor.execute(schema, query, ctx, deferredResolver = new ProductResolver).await should be (
        Map("data" →
          Map(
            "projectAll" → Vector(
              Map(
                "id" → "1",
                "variantIds" → Nil,
                "masterVariant" → Map("mixed" → "1"),
                "variants" → Vector(Map("id" → "2", "mixed" → "2"))),
              Map(
                "id" → "2",
                "variantIds" → Nil,
                "masterVariant" → Map("mixed" → "1"),
                "variants" → Nil)),
            "projectOne" → Vector(
              Map(
                "id" → "1",
                "variantIds" → Nil,
                "masterVariant" → Map("mixed" → "1"),
                "variants" → Vector(Map("id" → "2", "mixed" → "2"))),
              Map(
                "id" → "2",
                "variantIds" → Nil,
                "masterVariant" → Map("mixed" → "1"),
                "variants" → Nil)))))

      ctx.allProjections should be (
        Vector(
          ProjectedName("id", Vector.empty),
          ProjectedName("masterVariant.id", Vector.empty),
          ProjectedName("variants.id", Vector.empty),
          ProjectedName("master1", Vector(
              ProjectedName("mixed1", Vector.empty),
              ProjectedName("mixed2", Vector.empty))),
          ProjectedName("master2", Vector(
              ProjectedName("mixed1", Vector.empty),
              ProjectedName("mixed2", Vector.empty))),
          ProjectedName("variants",
            Vector(
              ProjectedName("id", Vector.empty),
              ProjectedName("mixed1", Vector.empty),
              ProjectedName("mixed2", Vector.empty)))))

      ctx.oneLevelprojections should be (
        Vector(
          ProjectedName("id", Vector.empty),
          ProjectedName("masterVariant.id", Vector.empty),
          ProjectedName("variants.id", Vector.empty),
          ProjectedName("master1", Vector.empty),
          ProjectedName("master2", Vector.empty),
          ProjectedName("variants", Vector.empty)))
    }

    "handle projected fields different at each level" in {
      val Success(query) = QueryParser.parse(
        """
          {
            projectAll {
              id
              masterVariant {
                categories {
                  name
                }
              }
            }
          }
        """)

      val ctx = new Ctx

      Executor.execute(schema, query, ctx, deferredResolver = new CategoryResolver).await should be (
        Map("data" →
          Map(
            "projectAll" → Vector(
              Map(
                "id" → "1",
                "masterVariant" →
                  Map("categories" → Vector(
                    Map("name" → "category 1"),
                    Map("name" → "category 2")
                  ))),
            Map("id" → "2",
              "masterVariant" →
              Map("categories" → Vector.empty))))))

      // There should be a way to define we want to select (id, typeId) from categories
      // and there should be a way to have only projections for the first level
//      ctx.allProjections should be (
//        Vector(
//          ProjectedName("id", Vector.empty),
//          ProjectedName("master1", Vector(
//            ProjectedName("categories", Vector(
//              ProjectedName("id", Vector.empty),
//              ProjectedName("typeId", Vector.empty))))),
//          ProjectedName("master2", Vector(
//            ProjectedName("categories", Vector(
//              ProjectedName("id", Vector.empty),
//              ProjectedName("typeId", Vector.empty)))))))

      ctx.allProjections should be (
        Vector(
          ProjectedName("id", Vector()),
          ProjectedName("master1", Vector(
            ProjectedName("categories.id", Vector(
              ProjectedName("name", Vector()))),
            ProjectedName("categories.typeId", Vector(
              ProjectedName("name",Vector()))))),
          ProjectedName("master2", Vector(
            ProjectedName("categories.id", Vector(
              ProjectedName("name", Vector()))),
            ProjectedName("categories.typeId", Vector(
              ProjectedName("name",Vector())))))))

      ctx.categoryProjections should be (
        Vector(
          ProjectedName("name", Vector.empty)))
    }
  }
}