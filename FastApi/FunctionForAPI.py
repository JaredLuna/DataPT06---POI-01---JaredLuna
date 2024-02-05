from fastapi import FastAPI
import pandas as pd

app = FastAPI()

#http://127.0.0.1:8000

def load_and_clean():
    # Declaración de funciones de limpieza
    def clean(x):
        x = x.replace('[', '')
        x = x.replace(']', '')
        x = x.replace("'", "")
        x = x.split(',')
        return x
    
    #Aquí se leen los parquet y se da un pequeño tratamiento para empezar a trabajar
    #Output steam dataframe
    Output_steam_df = pd.read_parquet('./Parquets/Output_steam_post_etl.parquet')
    Output_steam_df = Output_steam_df.drop('Unnamed: 0.1', axis= 1)
    Output_steam_df = Output_steam_df.drop('Unnamed: 0', axis= 1)
    Output_steam_df = Output_steam_df.drop('url', axis= 1)
    Output_steam_df = Output_steam_df.drop('reviews_url', axis= 1)
    Output_steam_df = Output_steam_df.drop('app_name', axis= 1)
    Output_steam_df['genres'] = Output_steam_df['genres'].apply(clean)
    Output_steam_df['tags'] = Output_steam_df['tags'].apply(clean)
    Output_steam_df['specs'] = Output_steam_df['specs'].apply(clean)

    #Australian items dataframe
    Australian_items_df = pd.read_parquet('./Parquets/Australian_items_post_etl.parquet')
    Australian_items_df = Australian_items_df.drop('Unnamed: 0.1', axis= 1)
    Australian_items_df = Australian_items_df.drop('Unnamed: 0', axis= 1)

    #Australian items expanded dataframe
    Australian_Items_Expanded_df = pd.read_parquet('./Parquets/australian_items_expanded.parquet')
    Australian_Items_Expanded_df = Australian_Items_Expanded_df.drop('Unnamed: 0', axis= 1)

    #Australian Reviews dataframe
    Australian_Reviews_df = pd.read_parquet('./Parquets/Australian_reviews_post_etl.parquet')
    Australian_Reviews_df = Australian_Reviews_df.drop('Unnamed: 0.1', axis= 1)
    Australian_Reviews_df = Australian_Reviews_df.drop('Unnamed: 0', axis= 1)

    #Australian Reviews expanded dataframe
    Australian_Reviews_Expanded_df = pd.read_parquet('./Parquets/australian_reviews_expanded.parquet')
    Australian_Reviews_Expanded_df = Australian_Reviews_Expanded_df.drop('Unnamed: 0', axis= 1)
    Australian_Reviews_Expanded_df['item_id'] = Australian_Reviews_Expanded_df['item_id'].astype(str)
    return Output_steam_df, Australian_items_df, Australian_Items_Expanded_df, Australian_Reviews_df, Australian_Reviews_Expanded_df

@app.get('/')
def index():
    return {'Primer proye to individual de DataScience para Henrey, by JL'}

@app.get('/PlayTimeGenre/{genre}')
def PlayTimeGenre(genero: str):

    Output_steam_df, Australian_items_df, Australian_Items_Expanded_df, Australian_Reviews_df, Australian_Reviews_Expanded_df = load_and_clean()

    def busca_genero(x):
        return genero in x
        
    #Copiamos el dataframe de outputsteam en esta variable
    df_generos = Output_steam_df

    #Este df contiene las lineas con el genero deseado
    generos_encontrados_df = df_generos[df_generos['genres'].apply(lambda z: busca_genero(z))]
    generos_encontrados_df['id'] = generos_encontrados_df['id'].astype(str)
    generos_encontrados_df['id'] = generos_encontrados_df['id'].str.replace(r'\.0$', '', regex=True)

    #Extraemos los IDS a buscar en australian_revies_expanded para poder encontrar las horas de juego
    ids_a_buscar = generos_encontrados_df['id']

    #Creamos un nuevo dataframe con los valores de nuestro coincidencias en ambos dataframes
    df_ids_coincidentes = Australian_Reviews_Expanded_df[Australian_Reviews_Expanded_df['item_id'].isin(ids_a_buscar)]

    #Creamos un dataframe con los ids y la sumas de sus horas
    df = df_ids_coincidentes
    df_suma_por_id = df.groupby('item_id')['playtime_2weeks'].sum().reset_index()

    #Extramemos la fila que contenga el valor de horas mayor para obtener su ID
    # Encontrar el índice del máximo
    indice_maximo = df_suma_por_id['playtime_2weeks'].idxmax()

    # Extraer la fila con el valor más alto
    fila_maximo = df_suma_por_id.loc[indice_maximo]
    IDfinal = fila_maximo['item_id']

    #Para sacar el año
    fecha_encontrada = generos_encontrados_df.loc[generos_encontrados_df['id'] == IDfinal, 'release_date'].values
    fecha_encontrada = fecha_encontrada[0].split('-')
    anio = fecha_encontrada[0]
    return {f'El año con más horas jugadas para el genero {genero} es: {anio}'}