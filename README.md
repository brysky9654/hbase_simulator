# Proyecto 3 Bases de datos 2 - hbaseSimulator

## Integrantes

- Mark Albrand
- Melissa Pérez
- Sara Echeverria
- Sebastian Juárez
- Jimena Hernández

## Sobre el proyecto
Desarrollo de una aplicación con interfaz gráfica que facilita la gestión y almacenamiento de datos estructurados como archivos columnares. Esta aplicación simula el funcionamiento de [HBase](https://hbase.apache.org/), implementando las funciones DDL y DML comúnmente utilizadas en HBase.

## Estructura propuesta

Para el proyecto se propone el uso de archivos JSON para el manejo de la metadata y el guardado de información. 
Para cada tabla se usarán dos archivos JSON, uno específico para la metadata, llamado ‘config.json’, y otro para guardar las filas de las tablas, llamado ‘hfile.json’.

### Estructura congif.json

``` json
{
    "enabled": True,
    "name": "Nombre de la tabla",
    "column_families": "cf1": {
        "version": "1",
        ...
    },
 }
 ```

Esta estructura es crucial para definir la configuración de las tablas y las familias de columna. enabled: Indica si la tabla está habilitada, es un valor booleano. name: El nombre de la tabla, es una cadena de texto que identifica la tabla en HBase. column_families: un objeto que define las familias de columnas de la tabla. Dentro de column_families se tiene version: Define el número de versiones que se almacenarán para cada celda en la familia de columnas.

### Estructura de Hfiles

``` json
{
        "1": {
            "column_fam_1": {
                "column_qualifier": {
                    "timestamp": "1",
                }
            },
            "column_fam_2": {
                "column_qualifier": {
                    "timestamp": "1",
                }
            }
        },
        "2": {
            "column_fam_1": {
                "column_qualifier": {
                    "timestamp": "1",
                }
            },
            "column_fam_2": {
                "column_qualifier": {
                    "timestamp": "1",
                }
            }
        },
      ...
}
 ```

Los Hfiles tienen una rowkey que representan las claves de fila en la tabla HBase, dentro de cada clave principal, hay familias de columnas donde cada familia de columnas agrupa varios calificadores de columnas que contiene atributos necesarios. Estos atributos son variables, pero de forma fija debe tener un timestamp que es un valor numérico que representa el momento en que se insertó el dato.

### Guardado de HFiles 

``` bash
--- tables
--- --- table1
--- --- --- config.json <- Disabled, regions...
--- --- --- regions
--- --- --- --- region1
--- --- --- --- --- data.json
--- --- --- --- --- index.json
...
--- --- --- --- regionN
--- --- --- --- --- data.json
--- --- --- --- --- index.json
...
--- --- tableN
--- --- --- config.json
--- --- --- regions
--- --- --- --- region1
--- --- --- --- --- data.json
--- --- --- --- --- index.json
```

### Funciones adicionales
#### Insert many
Esta función se utiliza para, dentro de una misma tabla, insertar una cantidad variable de filas, cada una con una column qualifier y un valor. El formato es el siguiente:

``` bash
insert_many 'table name'  {'row ','colfamily:colname','new value'} {'row ', 'colfamily:colname', 'new value'} ... {'row ', 'colfamily:colname', 'new value'}
```

##### Ejemplo

``` bash
insert_many 'employees' {'1','col1:salary',24} {'2','col1:salary',25} {'3','col1:salary',24}
```


#### Update many
Esta función se utiliza para, dentro de una misma tabla, actualizar todas las filas indicadas en un único column qualifier, con el valor dado. El formato es el siguiente:

``` bash
update_many 'table_name' 'colfamily:colname' value row row ... row
```

##### Ejemplo

``` bash
update_many 'employees' 'col1:salary' 200 '1' '2' '3'
```

