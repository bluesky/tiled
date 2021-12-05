from ariadne import ObjectType, QueryType, gql, make_executable_schema

type_defs = gql("""

    type Query {
        # datasets(uris: [String], tags: [String], limit: Int, skip: Int): [Dataset]!
        hello(msg: String!): String!
    }




# """)

query = QueryType()
# # I think what we want here is the root tree, and a way to register new query parts based on root tree

# # entry = root_tree.authenticated_as(current_user)


@query.field("hello")
def resolve_datasets(self, *_, msg=""):
    return msg


schema = make_executable_schema(type_defs, query)

# def set_root_tree(**kwarg