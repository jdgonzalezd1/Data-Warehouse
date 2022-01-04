from datetime import datetime  # Libreria para la manipulacion de fechas

import MySQLdb.connections as con  # Libreria para la conexion con bases MySQL


# Clase destinada para la conexion a bases de datos MySQL
# von los metodos que permiten la carga y extraccion de datos
class MySQLCon:

    # Contructor que recibe los datos necesarios para la conexion
    # a una base de datos con sus respectivas credenciales
    # y que a su vez inicia la conexion a la base de datos
    # Para las consultas e inserciones se crean listas con las sentencias requeridas
    def __init__(self, hostname, port, user, password, db):
        self.hostname = hostname
        self.port = port
        self.username = user
        self.password = password
        self.database = db
        self.condb = None
        self.connect()
        self.selectsODS = ["* FROM fecha;", "* FROM enfermedad;",
                           "municipio.*, departamento.nombre from municipio INNER JOIN departamento ON municipio.cod_departamento = departamento.cod_departamento;",
                           "persona.*, fallecimiento.asistencia_medica from persona INNER JOIN fallecimiento ON persona.id_persona = fallecimiento.persona;",
                           "* FROM lugar;",
                           "fallecimiento.fecha, fallecimiento.enfermedad, lugar.departamento, lugar.municipio FROM fallecimiento INNER JOIN lugar ON fallecimiento.lugar = lugar.id_lugar;",
                           "fallecimiento.fecha, fallecimiento.enfermedad, lugar.departamento, lugar.municipio FROM fallecimiento INNER JOIN lugar ON fallecimiento.lugar = lugar.id_lugar;",
                           "fallecimiento.fecha, fallecimiento.enfermedad, lugar.departamento, lugar.municipio,fallecimiento.asistencia_medica FROM fallecimiento INNER JOIN lugar ON fallecimiento.lugar = lugar.id_lugar;",
                           "fallecimiento.persona, fallecimiento.lugar, fallecimiento.enfermedad, fallecimiento.fecha FROM `fallecimiento`;"]
        self.insertBHDW = [("dim_fecha (fecha_completa,anio,mes,dia) VALUES (%s,%s,%s,%s);", "DIM FECHA"),
                           ("dim_enfermedad (nombre, descript) VALUES (%s,%s);", "DIM ENFERMEDAD"),
                           ("dim_municipio (id_municipio,departamento,municipio) VALUES (%s,%s,%s);", "DIM MUNICIPIO"),
                           (
                               "dim_persona (id_persona,sexo,fecha_nac,edad,seguridad_social,asistencia_medica) VALUES (%s,%s,%s,%s,%s,%s);",
                               "DIM PERSONA"),
                           ("dim_lugar (id_lugar,municipio,tipo_lugar,institucion) VALUES (%s,%s,%s,%s);", "DIM LUGAR"),
                           ("fact_defun_por_municipio (fecha,enfermedad,municipio,cant_defun) VALUES (%s,%s,%s,%s);",
                            "FACT_DEFUN_POR_MUNICIPIO"),
                           ("fact_defunciones_fecha (fecha,enfermedad, cantidad_fallecidos) VALUES (%s,%s,%s);",
                            "FACT_DEFUN_POR_FECHA"),
                           (
                               "fact_asis_medica (fecha,municipio,nombre,sum_asistencia_med,sum_sin_asistencia_med) VALUES (%s,%s,%s,%s,%s);",
                               "FACT_ASIS_MEDICA"),
                           ("fact_muerte (persona,lugar,nombre,fecha_completa) VALUES (%s,%s,%s,%s);",
                            "FACT_MUERTE")]
        self.tables = ["fact_muerte", "fact_asis_medica", "fact_defunciones_fecha", "fact_defun_por_municipio", "dim_lugar"
            , "dim_persona", "dim_municipio", "dim_enfermedad", "dim_fecha"]

    # Metodo para la conexion a la base de datos con la libreria
    # MySQLdb con los datos almancenados en el cosntructor de la instancia
    def connect(self):
        try:
            self.condb = con.Connection(host=self.hostname, port=self.port,
                                        user=self.username,
                                        passwd=self.password,
                                        db=self.database)
            cursor = self.condb.cursor()
            cursor.execute("SET lc_time_names = 'es_CO';")
            print("Conexión exitoso a " + self.database)
        except Exception as e:
            print("Error de conexión", e)

    # Metodo para la carga de datosen la BD, el cual recibe parte de la sentencia para la carga de los datos,
    # una lista con la tuplas de los datos a cargar y una cadena con el nombre de la tabla. La carga de los datos se
    # realiza con el metodo executemany, el cual usa la sentencia SQL y la lista que iterara para cargar los datos
    def load(self, sql, data, table):
        cursor = self.condb.cursor()
        try:
            cursor.executemany("INSERT IGNORE INTO " + sql, data)
            print("TABLA ", table, " CARGADA CON ", len(data),
                  " REGISTROS (Los registros pueden ser repetidos y por ende haber sido ignroados)")
            self.condb.commit()
        except Exception as e:
            print("Error de insercion", e)
            self.condb.rollback()

    def delete(self, table):
        cursor = self.condb.cursor()
        try:
            cursor.execute("DELETE FROM " + table)
            print("TABLA ", table, " VACIADA")
            self.condb.commit()
        except Exception as e:
            print("Error de eliminación", e)
            self.condb.rollback()

    # Metodo para la extraccion de datos de la BD, el cual un String para complementar la sentencia SELECT
    # y retornara los datos obtenidos por el cursor, los cuales vienen en una lista de tuplas
    def extract(self, sql):
        cursor = self.condb.cursor()
        try:
            cursor.execute("SELECT " + sql)
            return cursor.fetchall()
        except Exception as e:
            print("Error de consulta", e)

    # Metodo encargado de cerrar la conexion con la base de datos,
    #  conexion que se encuentra alamcenada en la variable condb
    def closeconection(self):
        try:
            self.condb.close()
            print('Desconexión exitosa a ' + self.database)
        except Exception as e:
            print("Error de desconexión", e)


# Clase encargada de las tranformaciones necesarias para la carga de los datos extraidos de la ODS
# para el DW.
class Tranformaciones:

    # Constructora sin parametros
    def __init__(self):
        pass

    # Metodo encargado de la busqueda de la posicion de un item en una lista de tuplas, de modo tal se compararn las
    # Posiciones de la tupla ingresada con las posiciones de cada una de las tuplas de la lista. Se retorna la posicion
    # de la tupla con el numero K de posiciones de la tupla. Se envia -1 si no se encuentra la tupla en la lista
    def search_item(self, l, c, lim):
        x = 0
        for i in l:
            k = 0
            for j in range(len(i) - lim):
                if i[j] == c[j]:
                    k = k + 1
                else:
                    break
            if k == len(i) - lim:
                return x
            x = x + 1
        return -1

    # Metodo para la trabformacion de los valores de municipios en donde se crea una tupla con una clase compuesta por
    # el cod del departamento y municipio (al igual que en el listado del DANE) y se incluye el nombre del departamento
    # y municipio. No se controlan duplicados
    def transform_municipe(self, lista):
        l = []
        for i in lista:
            clave = str(i[1]) + "." + str(i[0])
            aux = (clave, str(i[3]), str(i[2]))
            l.append(aux)
        return l

    # Metodo encargado de la creación de las tuplas ordenadas de los daros personales de los fallecidos
    def transform_person(self, lista):
        l = []
        for i in lista:
            aux = (i[0], i[3], i[1], i[2], i[4], i[5])
            l.append(aux)
        return l

    # Metodo encargado de la tranformación de las tuplas con los datos de los lugares de fallecimiento de las personas
    # en donde se congirua la clave del municipio y se organizan los datos extraidos del ODS
    def transform_place(self, lista):
        l = []
        for i in lista:
            clave = str(i[2]) + "." + str(i[3])
            aux = (i[0], clave, i[1], i[4])
            l.append(aux)
        return l

    # Metodo para la tranformacion de los datos de las defunciones por muncipio, en donde se contempla la creacion de
    # listas con el codigo del municipio, fecha y enfermedad, en donde se controlan los valores duplicados y se acunulan
    # los registros duplicados en la ultima posicion de cada lista.
    def transform_fact_def_mun(self, lista):
        l = []
        for i in lista:
            mun = str(i[2]) + "." + str(i[3])
            aux = [i[0], i[1], mun, 1]
            pos = self.search_item(l, aux, 1)
            if pos == -1:
                l.append(aux)
            else:
                a = l[pos]
                a[3] = a[3] + 1
                l[pos] = a
        return l

    # Metodo para la tranformación de los datos de defunciones por fecha, creando listas con la fecha y enfermedad,
    # controlando los valores duplicados y acumulandolos en la ultima posicion de la lista
    def transform_fact_def_fecha(self, lista):
        l = []
        for i in lista:
            aux = [i[0], i[1], 1]
            pos = self.search_item(l, aux, 1)
            if pos == -1:
                l.append(aux)
            else:
                a = l[pos]
                a[2] = a[2] + 1
                l[pos] = a
        return l

    # Metodo para la tranformacion de los datos de asistencia medica, creando listas con los valores de fecha, muncipio y
    # enfermedad, controlando si la persona recibio o no asistencia medica en las dos ultimas posiciones de cada lista.
    def transform_fact_asis_medica(self, lista):
        l = []
        for i in lista:
            mun = str(i[2]) + "." + str(i[3])
            aux = [i[0], mun, i[1], 0, 0]
            pos = self.search_item(l, aux, 2)
            if pos == -1:
                if i[4] == "SI":
                    aux[3] = aux[3] + 1
                else:
                    aux[4] = aux[4] + 1
                l.append(aux)
            else:
                a = l[pos]
                if i[4] == "SI":
                    a[3] = a[3] + 1
                else:
                    a[4] = a[4] + 1
                l[pos] = a
        return l


if __name__ == '__main__':
    # Creacion de la conexion a bona_health_dw
    bonaheathl_dw, ods = MySQLCon('localhost', 3306, 'root', 'password', 'bona_health_dw'), \
                         MySQLCon('localhost', 3306, 'root', 'password', 'ods_bona_health')
    if bonaheathl_dw.condb is not None and ods.condb is not None:
        print("INICIO DE CARGA DE DATOS A BONA_HEALTH_DW: ", datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        for i in bonaheathl_dw.tables:
            bonaheathl_dw.delete(i)
        # Instancia de la clase con las tranformaciones
        tranform = Tranformaciones()
        # ciclo for para la extracion de los datos con las sentencias Select para el ODS
        # se definen condiciones para los casos de tranformaciones requeridas
        for i in range(9):
            lista = ods.extract(ods.selectsODS[i])
            if i == 2:
                lista = tranform.transform_municipe(lista)
            elif i == 3:
                lista = tranform.transform_person(lista)
            elif i == 4:
                lista = tranform.transform_place(lista)
            elif i == 5:
                lista = tranform.transform_fact_def_mun(lista)
            elif i == 6:
                lista = tranform.transform_fact_def_fecha(lista)
            elif i == 7:
                lista = tranform.transform_fact_asis_medica(lista)
            # Carga de los datos a la tabla especificada para cada iteración
            bonaheathl_dw.load(bonaheathl_dw.insertBHDW[i][0], lista, bonaheathl_dw.insertBHDW[i][1])
        # Cierre de las conexiones al ODs y DW
        bonaheathl_dw.closeconection()
        ods.closeconection()
        print("FIN DE CARGA DE DATOS A BONA_HEALTH_DW: ", datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    else:
        print("NO SE PUDO CONECTAR A ", bonaheathl_dw.database)
