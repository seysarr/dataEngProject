import pandas as pd
from libs.neo4jClass import *
from pymongo import MongoClient

def get_recipes_from_mongo():
    mongo_client = MongoClient(
                    "mongodb://mongo:27017/",  
                    username='admin',
                    password='admin')
    db = mongo_client["FoodDB"]
    collection = db["recipes"]

    recipes = list(collection.find())
    mongo_client.close()
    return recipes


def delete_all(graph):
    # Supprimer tous les nœuds et relations
    query1 = """
    MATCH (n)
    DETACH DELETE n
    """
    graph.run_query(query1)
    print("Tous les nœuds et relations ont été supprimés.")



def move_to_graph_DB(user, password):
    graph = Neo4jConnector("bolt://neo4j:7687", user, password)
    print("successfully connected!!!")
    delete_all(graph)
    # setup_constraints(graph) # to uncomment
    recipes = get_recipes_from_mongo()
    fill_data_neo4j(recipes, graph)
    print("Database successfully populated !!!")
    graph.close()


def setup_constraints(graph):
    constraints = [
        "CREATE CONSTRAINT FOR (r:Recipe) REQUIRE r.id IS UNIQUE",
        "CREATE CONSTRAINT FOR (i:Ingredient) REQUIRE i.name IS UNIQUE",
        "CREATE CONSTRAINT FOR (d:Diet) REQUIRE d.name IS UNIQUE",
        "CREATE CONSTRAINT FOR (h:HealthEffect) REQUIRE h.name IS UNIQUE",
        "CREATE CONSTRAINT FOR (n:Nutrient) REQUIRE n.type IS UNIQUE"
    ]
    for query in constraints:
        try:
            graph.run_query(query)
            print(f"Constraint added: {query}")
        except Exception as e:
            print(f"Error adding constraint: {query} - {e}")


def fill_data_neo4j(mongo_data, graph, batch_size=100):
    for i in range(0, len(mongo_data), batch_size):
        batch = mongo_data[i:i+batch_size]
        
        query = """
        UNWIND $batch AS recipe
        MERGE (r:Recipe {id: recipe.id})
        SET r.title = recipe.title, r.description = recipe.description, 
            r.duration = recipe.duration, r.url = recipe.url, r.serves = recipe.serves
        
        WITH r, recipe
        UNWIND recipe.ingredients AS ingredient
        MERGE (ing:Ingredient {name: ingredient})
        MERGE (r)-[:CONTAINS]->(ing)
        
        WITH r, recipe
        UNWIND recipe.diets AS diet
        MERGE (d:Diet {name: diet})
        MERGE (r)-[:APPLICABLE_TO]->(d)
        
        WITH r, recipe
        UNWIND recipe.health_effects AS effect
        MERGE (h:HealthEffect {name: effect})
        MERGE (r)-[:IMPACTS]->(h)
        
        WITH r, recipe
        UNWIND recipe.nutrients AS nutrient
        MERGE (n:Nutrient {name: nutrient.type})
        SET n.quantity = nutrient.quantity, n.unit = nutrient.unit
        MERGE (r)-[:HAS_NUTRIENT]->(n)
        """
        
        params = {
            "batch": [
                {
                    "id": recipe["new_recipe_id"],
                    "title": recipe["title"],
                    "description": recipe["description"],
                    "duration": recipe["duration"],
                    "url": recipe["recipe_url"],
                    "serves": recipe["serves"],
                    "ingredients": recipe.get("ingredients", []),
                    "diets": recipe.get("diets", []),
                    "health_effects": recipe.get("health_effects", []),
                    "nutrients": [
                        {"type": nutrient, "quantity": recipe[f"{nutrient} [{unit}]"], "unit": unit}
                        for nutrient, unit in [
                            ("calories", "cal"), ("totalFat", "g"), ("saturatedFat", "g"),
                            ("cholesterol", "mg"), ("sodium", "mg"), ("totalCarbohydrate", "g"),
                            ("dietaryFiber", "g"), ("sugars", "g"), ("protein", "g")
                        ]
                    ]
                } for recipe in batch
            ]
        }
        
        graph.run_query(query, params)



# def fill_data_neo4j(mongo_data, graph):
#     for recipe in mongo_data:
        
#         query = """
#         MERGE (r:Recipe {id: $id})
#         SET r.title = $title, r.description = $description, r.duration = $duration, r.url = $url, r.serves = $serves
        
#         WITH r
#         UNWIND $ingredients AS ingredient
#         MERGE (ing:Ingredient {name: ingredient})
#         MERGE (r)-[:CONTAINS]->(ing)
        
#         WITH r
#         UNWIND $diets AS diet
#         MERGE (d:Diet {name: diet})
#         MERGE (r)-[:APPLICABLE_TO]->(d)
        
#         WITH r
#         UNWIND $health_effects AS effect
#         MERGE (h:HealthEffect {name: effect})
#         MERGE (r)-[:IMPACTS]->(h)
        
#         WITH r
#         UNWIND $nutrients AS nutrient
#         MERGE (n:Nutrient {type: nutrient.type})
#         SET n.quantity = nutrient.quantity, n.unit = nutrient.unit
#         MERGE (r)-[:HAS_NUTRIENT]->(n)
#         """
        

#         params = {
#             "id": recipe["new_recipe_id"],
#             "title": recipe["title"],
#             "description": recipe["description"],
#             "duration": recipe["duration"],
#             "url": recipe["recipe_url"],
#             "serves": recipe["serves"],
#             "ingredients": recipe.get("ingredients", []),
#             "diets": recipe.get("diets", []),
#             "health_effects": recipe.get("health_effects", []),
#             "nutrients": [
#                 {"type": nutrient, "quantity": recipe[f"{nutrient} [{unit}]"], "unit": unit}
#                 for nutrient, unit in [
#                     ("calories", "cal"), ("totalFat", "g"), ("saturatedFat", "g"),
#                     ("cholesterol", "mg"), ("sodium", "mg"), ("totalCarbohydrate", "g"),
#                     ("dietaryFiber", "g"), ("sugars", "g"), ("protein", "g")
#                 ]
#             ]
#         }
        
#         graph.run_query(query, params)
