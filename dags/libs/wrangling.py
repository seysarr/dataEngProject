import pandas as pd
from pymongo import MongoClient
import csv
import ast
import re

def sample_hummus(output_folder, epoch):
    df=pd.read_csv(f'{output_folder}/pp_recipes.csv', low_memory=False)
    healthy_recipes = df[df['nutri_score'] >= 0.75]
    small_df = healthy_recipes.sample(n=3000, random_state=50)  # Keep exactly 3000 rows
    output_file = f'{output_folder}/sampled_data_{str(epoch)}.csv'
    small_df.to_csv(output_file, index=False, encoding='utf-8')


def rearrange_broken_lines(output_folder, epoch):
    input_file = f'{output_folder}/sampled_data_{str(epoch)}.csv'
    output_file = f'{output_folder}/sampled_data_fixed_{str(epoch)}.csv'
    
    with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
        lines = f_in.readlines()
        current_line = lines[0]  # keep headers

        for line in lines[1:]:
            if line.strip().split(',')[0].isdigit():
                f_out.write(current_line)
                current_line = line
            else:
                current_line = current_line.strip() + ' ' + line.strip()

        f_out.write(current_line)



def clean_data(output_folder, epoch):
    hummus_df = pd.read_csv(f'{output_folder}/sampled_data_fixed_{str(epoch)}.csv', on_bad_lines='skip') # some lines can't be parsed after merge
    
    spooncular_df = pd.read_csv(f'{output_folder}/spooncular_recipes_init_{str(epoch)}.csv', low_memory=False)
    # spooncular_df = pd.read_csv(f'{output_folder}/spooncular_recipes_init_1734807676.csv') # For test
    
    hummus_df = hummus_df.drop(['Unnamed: 0','new_recipe_id','author_id','last_changed_date','food_kg_locator','new_author_id','servingsPerRecipe','servingSize [g]','caloriesFromFat [cal]',
                                'average_rating','number_of_ratings','direction_size', 'ingredients_sizes','who_score','fsa_score','nutri_score','normalization_comment',
                                'ingredient_food_kg_urls'],axis=1)
    hummus_df = hummus_df.rename(columns={'ingredients': 'ingredients_detailed', 'ingredient_food_kg_names': 'ingredients'})
    
    hummus_no_duplicates_df = hummus_df.drop_duplicates()
    hummus_clean_df = hummus_no_duplicates_df.dropna()
    spooncular_no_duplicates_df = spooncular_df.drop_duplicates()
    spooncular_clean_df = spooncular_no_duplicates_df.dropna()
    
    hummus_clean_df.to_csv(f'{output_folder}/hummus_cleaned_{str(epoch)}.csv', index=False)
    spooncular_clean_df.to_csv(f'{output_folder}/spooncular_cleaned_{str(epoch)}.csv', index=False)


def convert_string_to_list(value):
    if isinstance(value, str):
        try:
            string_list = ast.literal_eval(value)
            return [s.strip() for s in string_list if s and s.strip()]
        except (ValueError, SyntaxError):
            return []
    return value if isinstance(value, list) else []


def diet_check(ingredient):
    ingredient = ingredient.lower().strip()
    diet_keywords = {
        'Vegetarian': ['vegetables?', 'fruits?', 'grains?', 'legumes?', 'tofu', 'dairy', 'eggs?', 'milk', 'cheese', 'yogurt', 'butter'],
        'Vegan': ['vegetables?', 'fruits?', 'grains?', 'legumes?', 'tofu', 'plant-based milk', 'nutritional yeast', 'seitan', 'tempeh'],
        'Keto': ['meat', 'fish', 'cheese', 'eggs?', 'butter', 'avocado', 'oils?', 'leafy greens', 'nuts', 'seeds'],
        'Paleo': ['meat', 'fish', 'fruits?', 'vegetables?', 'nuts', 'seeds', 'eggs?', 'honey', 'coconut oil'],
        'Gluten-Free': ['rice', 'quinoa', 'corn', 'potatoes?', 'fruits?', 'vegetables?', 'meat', 'fish', 'eggs?', 'nuts', 'seeds']
    }
    
    non_vegetarian = ['meat', 'beef', 'pork', 'chicken', 'turkey', 'lamb', 'veal', 'fish', 'seafood', 'shellfish', 'anchovies', 'gelatin', 'lard', 'tallow', 'rennet']
    non_vegan = non_vegetarian + ['eggs?', 'milk', 'cheese', 'butter', 'cream', 'yogurt', 'honey', 'whey', 'casein', 'lactose']
    non_keto = ['sugar', 'flour', 'bread', 'pasta', 'rice', 'potatoes?', 'corn', 'beans', 'lentils', 'fruits?']
    non_paleo = ['grains?', 'legumes?', 'dairy', 'processed foods?', 'refined sugars?', 'vegetable oils?']
    gluten_containing = ['wheat', 'barley', 'rye', 'oats', 'flour', 'bread', 'pasta', 'cereal', 'beer']

    matches = {}
    for diet, keywords in diet_keywords.items():
        matches[diet] = any(re.search(rf'\b{keyword}\b', ingredient) for keyword in keywords)
    
    # Verify excluded ingredients
    if matches['Vegetarian']:
        matches['Vegetarian'] = not any(re.search(rf'\b{keyword}\b', ingredient) for keyword in non_vegetarian)
    if matches['Vegan']:
        matches['Vegan'] = not any(re.search(rf'\b{keyword}\b', ingredient) for keyword in non_vegan)
    if matches['Keto']:
        matches['Keto'] = not any(re.search(rf'\b{keyword}\b', ingredient) for keyword in non_keto)
    if matches['Paleo']:
        matches['Paleo'] = not any(re.search(rf'\b{keyword}\b', ingredient) for keyword in non_paleo)
    if matches['Gluten-Free']:
        matches['Gluten-Free'] = not any(re.search(rf'\b{keyword}\b', ingredient) for keyword in gluten_containing)
    
    return [diet for diet, matched in matches.items() if matched]


def check_ingredients(ingredients):
    if not isinstance(ingredients, list):
        return []
    diets = set()
    for ingredient in ingredients:
        if isinstance(ingredient, str):
            diets.update(diet_check(ingredient))
    return list(diets)


def add_diets_column(output_folder, epoch):
    df = pd.read_csv(f'{output_folder}/hummus_cleaned_{epoch}.csv')
    df['ingredients'] = df['ingredients'].apply(convert_string_to_list)
    
    df['diets'] = df['ingredients'].apply(check_ingredients)
    df_final = df[df['diets'].apply(bool)]
    df_final.to_csv(f'{output_folder}/hummus_with_diets_{epoch}.csv', index=False)


def merge_recipes(output_folder, epoch):
    df1 = pd.read_csv(f'{output_folder}/spooncular_cleaned_{epoch}.csv')
    df2 = pd.read_csv(f'{output_folder}/hummus_with_diets_{epoch}.csv')

    merged_df = pd.concat([df1, df2], ignore_index=True)
    output_file = f'{output_folder}/recipes_merged_{str(epoch)}.csv'
    merged_df.to_csv(output_file, index=False)


def determine_health_effects(row):
    effects = []
    
    # Calories (based on 25% of recommended daily intake)
    if row['calories [cal]'] < 500:  # For women, 2000 per day
        effects.append("Weight control")
        effects.append("Flat belly")
    elif 500 <= row['calories [cal]'] <= 650:  # For men, 2600 Moderately active average a day
        effects.append("Nutritional balance")
    
    # Fats (30% of meal calories)
    calories_from_fat = row['totalFat [g]'] * 9
    if row['calories [cal]'] != 0.0 and calories_from_fat / row['calories [cal]'] <= 0.30:
        effects.append("Cardiovascular health")
        effects.append("Flat belly")
    
    # Saturated fats (less than 10% of meal calories)
    calories_from_sat_fat = row['saturatedFat [g]'] * 9
    if row['calories [cal]'] != 0.0 and calories_from_sat_fat / row['calories [cal]'] <= 0.10:
        effects.append("Saturated fat reduction")
        effects.append("Flat belly")
    
    # Carbohydrates (55% of meal calories), naturally sugar, starch(fr=amidon), fiber
    calories_from_carbs = row['totalCarbohydrate [g]'] * 4
    if row['calories [cal]'] != 0.0 :
        if 0.50 <= calories_from_carbs / row['calories [cal]'] <= 0.55:
            effects.append("Carbohydrate balance")
        elif calories_from_carbs / row['calories [cal]'] < 0.50:
            effects.append("Flat belly")
    
    # Fiber (at least 8g per meal, based on 30g per day)
    if row['dietaryFiber [g]'] >= 8:
        effects.append("Digestive health")
        effects.append("Diabetes prevention")
        effects.append("Flat belly")
    
    # Proteins (15-20% of meal calories)
    calories_from_protein = row['protein [g]'] * 4
    if row['calories [cal]'] != 0.0 :
        if 0.15 <= calories_from_protein / row['calories [cal]'] <= 0.20:
            effects.append("Balanced protein intake")
            effects.append("Muscle strengthening")
        elif calories_from_protein / row['calories [cal]'] > 0.20:
            effects.append("Enhanced muscle strengthening")
    
    # Sodium (less than 600mg per meal, based on 2400mg per day)
    if row['sodium [mg]'] < 600:
        effects.append("Blood pressure control")
    
    return str(list(set(effects))) if effects else "['No notable effect']"


def add_health_effects_column(output_folder, epoch):
    df = pd.read_csv(f'{output_folder}/recipes_merged_{str(epoch)}.csv')
    # df = pd.read_csv(f'{output_folder}/recipes_merged_1735044849.csv') # For test
    df['health_effects'] = df.apply(determine_health_effects, axis=1)
    df['new_recipe_id'] = range(len(df))
    df.to_csv(f'{output_folder}/recipes_with_healthEffects_{epoch}.csv', index=False)


def save_to_mongoDB(output_folder, epoch):
    client = MongoClient(
                    "mongodb://mongo:27017/",  
                    username='admin',
                    password='admin')
    db_name = "FoodDB"
    collection_name = "recipes"
    csv_file_path = f'{output_folder}/recipes_with_healthEffects_{epoch}.csv'
    # csv_file_path = f'{output_folder}/recipes_with_healthEffects_1735044849.csv' # For test
    
    db = client[db_name]
    collection = db[collection_name]
    
    data = pd.read_csv(csv_file_path)
    list_columns = ['diets', 'ingredients', 'health_effects']
    
    for col in list_columns:
        if col in data.columns:
            data[col] = data[col].apply(convert_string_to_list)


    records = data.to_dict(orient='records')
    for record in records:
        new_recipe_id = record['new_recipe_id']
        if collection.count_documents({'_id': new_recipe_id}) == 0:
            record['_id'] = new_recipe_id
            collection.insert_one(record)
        else:
            print(f"Document with Id {new_recipe_id} already exists, non added.")

    client.close()

