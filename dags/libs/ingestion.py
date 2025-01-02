def get_spooncular_recipes(output_folder, epoch):
    import csv
    import re

    field_names = ['recipe_id', 'title', 'description', 'duration', 'directions', 'ingredients', 'ingredients_detailed', 'serves', 'recipe_url',
                'tags', 'calories [cal]', 'totalFat [g]', 'saturatedFat [g]',
        'cholesterol [mg]', 'sodium [mg]', 'totalCarbohydrate [g]',
        'dietaryFiber [g]', 'sugars [g]', 'protein [g]', 'diets'
        ]
    recipes = []
    output_file = f'{output_folder}/spooncular_recipes_init_{str(epoch)}.csv'
    api_data = get_api_data()

    for recipe in api_data:
        nutrition = recipe.get("nutrition", {}).get("nutrients", [])
        nutrients = {nutrient['name']: nutrient['amount'] for nutrient in nutrition}

        recipe_item = {
            'recipe_id': recipe.get('id'),
            'title': recipe.get('title'),
            'description': re.sub(r'<.*?>', '', recipe.get('summary')),
            'duration':recipe.get('readyInMinutes'),
            'directions': [
                step.get('step', '')
                for step in recipe.get('analyzedInstructions', [{}])[0].get('steps', [])
            ],
            'ingredients': [ingredient.get('nameClean', ingredient.get('name', 'Unknown'))
                            for ingredient in recipe.get('extendedIngredients', [])],
            'ingredients_detailed': recipe['analyzedInstructions'][0]['steps'],
            'serves':recipe.get('servings'),
            'recipe_url': recipe.get('sourceUrl'),
            'tags': recipe.get('dishTypes') + recipe.get('cuisines') + recipe.get('occasions'),
            'calories [cal]': nutrients.get('Calories'),
            'totalFat [g]': nutrients.get('Fat'),
            'saturatedFat [g]': nutrients.get('Saturated Fat'),
            'cholesterol [mg]': nutrients.get('Cholesterol'), 
            'sodium [mg]': nutrients.get('Sodium'),
            'totalCarbohydrate [g]': nutrients.get('Carbohydrates'),
            'dietaryFiber [g]': nutrients.get('Fiber'),
            'sugars [g]': nutrients.get('Sugar'),
            'protein [g]': nutrients.get('Protein'),
            'diets': recipe.get('diets')
        }
        
        recipes.append(recipe_item)
        
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=field_names)
        writer.writeheader()
        writer.writerows(recipes)
    return True

def get_api_data():
    import requests

    api_key = "b8f72c2132624ceca52c9e0408ff5cc9"
    base_url = "https://api.spoonacular.com/recipes"
    total_results = []
    offset = 0
    limit = 100
    nb_request=0
    size = 0
    desired_count = 300
    
    while (size < desired_count):
        params = {
            "apiKey": api_key,
            "number": limit,
            "offset": offset,
            "diet": "healthy",
            "addRecipeInformation": True,
            "fillIngredients": True,
            "instructionsRequired": True,
            "addRecipeNutrition": True,
            "sort": "healthiness",
            "sortDirection": "desc"
        }
        response = requests.get(f"{base_url}/complexSearch", params=params)
        data = response.json()
        recipes = data.get('results', [])
        
        total_results.extend(recipes)
        nb_request += 1
        size = len(total_results)
        
        if len(data['results']) < limit:
            break
        
        offset += limit

    return total_results


def check_task_status(success_task_id, **context):
    dag_run = context['dag_run']
    task = context['task']
    upstream_task_ids = task.upstream_task_ids
    
    for task_id in upstream_task_ids:
        upstream_ti = dag_run.get_task_instance(task_id)
        if upstream_ti.state not in ['success', 'skipped']:
            if success_task_id == 'WRANGLING':
                return 'handle_ingestion_error'
            elif success_task_id == 'PROD':
                return 'handle_wrangling_error'
            else:
                return 'handle_prod_error'
    
    return success_task_id
