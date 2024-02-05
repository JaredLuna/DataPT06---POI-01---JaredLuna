import pandas as pd

def main():
    #Funciones del proyecto

    #Funcion 01
    def PlayTimeGenre(genero):

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
        año = fecha_encontrada[0]
        return año

    #Funcion 02
    def UserForGenre(genero):

        def busca_genero(x):
            return genero in x

        def busca_usuario_juegos(z):
            return Id_final_user in z

        def get_only_yeas(y):
            y = y.split('-')
            return y[0]

        def clasificador_anual(years_list):
            
            def year_search(i, x):
                return i in x

            lista_años_final = []

            for i in years_list:
                lista_años_final.append(i)
                juegos_año_i = datos_del_usuario_con_fehca_Df[datos_del_usuario_con_fehca_Df['release_date'].apply(lambda x: year_search(i, x))]
                id_juegos_año_i = juegos_año_i['id']
                juegos_user_hours_i = juegos_del_usuario_df[juegos_del_usuario_df['item_id'].isin(id_juegos_año_i)]
                horas_juego_añi_i = juegos_user_hours_i['playtime_2weeks'].sum()
                lista_años_final.append(horas_juego_añi_i)
            
            return lista_años_final

        def clasificador(respuesta):
            # Convertir la lista en una lista de tuplas (año, número)
            tuples_list = [(respuesta[i], respuesta[i+1]) for i in range(0, len(respuesta), 2)]

            # Ordenar la lista de tuplas por el año en orden descendente
            sorted_tuples = sorted(tuples_list, key=lambda x: int(x[0]), reverse=True)

            # Convertir la lista de tuplas ordenada en una lista plana de 2xn
            result_list = [item for sublist in sorted_tuples for item in sublist]
            return result_list

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
        df_suma_por_id = df.groupby('user_id')['playtime_2weeks'].sum().reset_index()

        #Extramemos la fila que contenga el valor de horas mayor para obtener su ID
        # Encontrar el índice del máximo
        indice_maximo = df_suma_por_id['playtime_2weeks'].idxmax()

        # Extraer la fila con el valor más alto
        fila_maximo = df_suma_por_id.loc[indice_maximo]
        Id_final_user = fila_maximo['user_id']

        #Ya tenemos el Id del usuario que mas horas jugó, ahora hay que extraer el id de los items que jugo, sacar la suma de horas por año
        juegos_del_usuario_df = Australian_Reviews_Expanded_df[Australian_Reviews_Expanded_df['user_id'].apply(lambda z : busca_usuario_juegos(z))]

        #ahora tenemos que extraer el item_name para buscarlo en df genero_encontrados, para extraer los años
        nombre_juegos = juegos_del_usuario_df['item_name']

        #Extraemos las filas con los juegos
        datos_del_usuario_con_fehca_Df = generos_encontrados_df[generos_encontrados_df['title'].isin(nombre_juegos)]
        datos_del_usuario_con_fehca_Df['release_date'] = datos_del_usuario_con_fehca_Df['release_date'].apply(lambda n: get_only_yeas(n))
        years_list = datos_del_usuario_con_fehca_Df['release_date'].str.extract('(\d+)')[0].unique().tolist()

        #Extraemos el Id del juego y la fecha
        fecha_id_juego_df = datos_del_usuario_con_fehca_Df[['release_date', 'id']]
        items_ids = fecha_id_juego_df['id']

        #Extraemos las horas jugadas por el id del juego y del usuario 
        horas_juego_user_gameId = Australian_Reviews_Expanded_df.loc[(Australian_Reviews_Expanded_df['user_id'] == Id_final_user) & (Australian_Reviews_Expanded_df['item_id'].isin(items_ids))]

        respuesta = clasificador_anual(years_list)
        clasificacion_anual =clasificador(respuesta)
        return (Id_final_user, clasificacion_anual)
    
    #Funcion 03
    def UsersRecommend(x):
        df_reviews = pd.read_parquet('./Parquets/user_reviews.parquet')
        año = x
        df_reviews['item_id']= df_reviews['item_id'].astype(str)
        #Extraemos los juegos de ese año
        juegos_anio_i = Output_steam_df[Output_steam_df['release_date'].apply(lambda x: año in x)]
        juegos_anio_i['id'] = juegos_anio_i['id'].astype(str).str.replace(r'\.0$', '', regex=True)
        #De las reviews extraemos solo los juegos del año que nos interesa
        ids = juegos_anio_i['id']
        reviews_juegos_x_anio_i = df_reviews[df_reviews['item_id'].isin(ids)]
        #Ahora contamos por juego la cantidad de reseñas que lo recomiendan
        conteo_reviews_x_juego = reviews_juegos_x_anio_i[reviews_juegos_x_anio_i['recommend'] == True].groupby('item_id')['recommend'].sum().reset_index()
        #Ahora sacamos el top 3 en dataframe
        top_3_reviews_df = conteo_reviews_x_juego.nlargest(3, 'recommend')
        top3_ids = top_3_reviews_df['item_id']
        #Ya tenemos el ID del top 3 de los juegos, ahora solo extraemos los nombres
        datos_juegos_top3 = juegos_anio_i[juegos_anio_i['id'].isin(top3_ids)]
        top3_nombres = datos_juegos_top3['title'].tolist()
        return top3_nombres
    
    #Funcion 4
    def UsersNotRecommend(x):
        df_reviews = pd.read_parquet('./Parquets/user_reviews.parquet')
        año = x
        df_reviews['item_id']= df_reviews['item_id'].astype(str)
        #Extraemos los juegos de ese año
        juegos_anio_i = Output_steam_df[Output_steam_df['release_date'].apply(lambda x: año in x)]
        juegos_anio_i['id'] = juegos_anio_i['id'].astype(str).str.replace(r'\.0$', '', regex=True)
        #De las reviews extraemos solo los juegos del año que nos interesa
        ids = juegos_anio_i['id']
        reviews_juegos_x_anio_i = df_reviews[df_reviews['item_id'].isin(ids)]
        #Ahora contamos por juego la cantidad de reseñas que lo recomiendan
        conteo_reviews_x_juego = reviews_juegos_x_anio_i[reviews_juegos_x_anio_i['recommend'] == True].groupby('item_id')['recommend'].sum().reset_index()
        #Ahora sacamos el top 3 en dataframe
        top_3_reviews_df = conteo_reviews_x_juego.tail(3)
        top3_ids = top_3_reviews_df['item_id']
        #Ya tenemos el ID del top 3 de los juegos, ahora solo extraemos los nombres
        datos_juegos_top3 = juegos_anio_i[juegos_anio_i['id'].isin(top3_ids)]
        top3_nombres = datos_juegos_top3['title'].tolist()
        return top3_nombres

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
    print('Se termino lo que se vendía')
    

if __name__ == '__main__':
    main()