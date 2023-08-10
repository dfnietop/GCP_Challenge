# GCP_Challenge
1. Arquitectura Utilizada:
   * GCP STORAGE
   * GCP Functions
     * PYTHON 3.8
   * GCP Biguery
   * Tableau Online
2. Adquisicion de Datos:
   Para la adquisicion de datos se crea un bucket en GCP llamado bucket-gcp-challenge 
   con el fin de cargar los datos CSV de :
   1. Clientes: Informacion relacionada a las caracteristicas del cliente
      1. Personal
      2. Econonico
      3. Geografico
      4. Tecnologico
   2. Compras: Informacion relacionada al comportamiento de clientes con productos
      de telefonia movil, relacion de negocio con clientes y productos
      1. Informacion historica de compras de productos de telefonia movil
      2. Relacion de clientes y sus compras 
   3. Productos: Informacion de los productos de telefonia movil segun su clasificacion:
      1. Prepago
      2. Postpago
3. Limpieza y tranformacion y carga de Datos:
El proceso de se divide en dos fases:
   1. GCP-Function para procesar: 
      La responsabilidad principal de la funcion es: 
      1. Detonarse una vez se encuentre un archivo nuevo dentro del bucket
      2. Realizar transformacion y limpieza de datos con Python:
         1. Se utiliza pandas para realizar transformacion y limpieza
         2. Se utilizan expresiones regulares para identificar los errores de datos esecenciales
      3. Realizar el llamado a BigQuery          
   2. GCP-BigQuery para almacenar: 
      1. Una vez cargado y detonado la funcion de activacion function-1 se crean los dataset de la siguiente forma:
         gcp-challenge-395021.{fechaEjecucion}+'BBDMovil'
      2. Una vez se tiene los datos limpios y procesados en dataframe de pandas
         se realiza el proceso de creacion de Objetos tipo tabla:
         1. Clientes
         2. Compras
         3. Producto.
   
3. Visualizacion de Datos:
Se utiliza Tableau online para realizar la visualizacion alojada en el GCP-BigQuery creando 2 tableros:
   1. Reporte de Ventas Latam: Representa el analisis de los clientes a nivel geografico teniendo en cuenta 3 aspectos:
      1. Segmentacion Financiera (compras)
      2. Segmentacion Personal (Edad, preferencias, estado civil)
      3. Segmentacion Geografica
   2. Reporte Cumplimiento Latam: Representa el cumplimiento del negocio durante el 
      periodo de adquisicion de productos de telefonia a nivel Latinoamerica creando KPI'S orientados a:
      1. Compras de Usuario: se muestra cumplimiento de cantidad de ventas y el valor de las mismas por mes
         y su variacion respectiva al mes anterior.
      2. Capacidad tecnologica: se muestra cumplimiento de cantidad de Datos ofrecidos en el mes
         y su variacion respectiva al mes anterior.
   