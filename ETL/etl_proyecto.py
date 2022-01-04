from datetime import datetime  # Libreria para la manipulacion de fechas

import MySQLdb.connections as con  # Libreria para la conexion con bases MySQL
import pandas as pd  # Libreria para la manipulacion de archivos CSV
from dateutil.parser import parse  # Metodo para formatear fechas en Strings sin indicar el formato de destino
from dateutil.relativedelta import relativedelta  # Metodo para encontrar la diferencia entre dos fechas


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
        self.insertsODS = [("ods_bona_health.departamento (cod_departamento,nombre) VALUES (%s, %s)", "DEPARTAMENTO"),
                           ("ods_bona_health.institucion ( cod_institucion,nombre) VALUES (%s, %s)", "INSTITUCION"),
                           ("ods_bona_health.municipio (cod_municipio,cod_departamento,nombre) VALUES (%s, %s, %s)",
                            "MUNICIPIO"),
                           ("ods_bona_health.lugar (id_lugar,tipo_lugar,departamento,municipio,institucion) VALUES (%s, %s, %s,%s, %s)",
                               "LUGAR"),
                           ("ods_bona_health.fecha (fecha_completa,año,mes,dia) VALUES (%s, %s, %s,%s)", "FECHA"),
                           ("ods_bona_health.persona (id_persona,fecha_nam,edad,sexo,seg_social) VALUES (%s, %s, %s,%s,%s)",
                               "PERSONA"),
                           (
                               "ods_bona_health.fallecimiento (lugar,fecha,persona,enfermedad,asistencia_medica) VALUES (%s,%s, %s,%s,%s)",
                               "FALLECIMIENTO")]
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
                               "fact_asis_medica (fecha,municipio,enfermedad,sum_asistencia_med,sum_sin_asistencia_med) VALUES (%s,%s,%s,%s,%s);",
                               "FACT_ASIS_MEDICA"),
                           ("fact_muerte (persona,lugar,enfermedad,fecha_completa) VALUES (%s,%s,%s,%s);",
                            "FACT_MUERTE")]

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


## Clase encarga de la manipulacion de los archivos csv
# haciendo uso de la libreria Pandas
class ArchivoMort:

    # Contructor de la clase, el cual recibe el nombre del archivo ubicado en la carpeta
    # DATASETS y el nombre de la enfermedad asociada a este. Dentro de este metodo se incluyen
    # parametros como separador de datos, nombres de las columnas y se crea el dataframe a manipular
    def __init__(self, nombre, enfermedad):
        self.ruta = 'DATASETS/' + nombre
        self.sep = ';'
        self.names = ['COD_DPTO', 'COD_MUNIC', 'A_DEFUN', 'COD_INSP', 'SIT_DEFUN', 'OTRSITIODE',
                      'COD_INST', 'NOM_INST', 'TIPO_DEFUN', 'FECHA_DEF', 'ANO', 'MES', 'HORA', 'MINUTOS',
                      'SEXO', 'FECHA_NAC', 'EST_CIVIL', 'EDAD', 'NIVEL_EDU', 'ULTCURFAL', 'MUERTEPORO',
                      'SIMUERTEPO', 'OCUPACION', 'IDPERTET', 'IDPUEBIN', 'N_IDPUEBIN', 'CODPRES', 'CODPTORE',
                      'CODMUNRE', 'AREA_RES', 'BARRIOFAL', 'COD_LOCA', 'CODIGO', 'VEREDAFALL', 'SEG_SOCIAL',
                      'IDADMISALU', 'IDCLASADMI', 'PMAN_MUER', 'CONS_EXP', 'MU_PARTO', 'T_PARTO', 'TIPO_EMB',
                      'T_GES', 'PESO_NAC', 'EDAD_MADRE', 'N_HIJOSV', 'N_HIJOSM', 'EST_CIVM', 'NIV_EDUM',
                      'ULTCURMAD', 'EMB_FAL', 'EMB_SEM', 'EMB_MES', 'MAN_MUER', 'COMOCUHEC', 'CODOCUR',
                      'CODMUNOC', 'DIROCUHEC', 'LOCALOCUHE', 'C_MUERTE', 'C_MUERTEB', 'C_MUERTEC', 'C_MUERTED',
                      'C_MUERTEE', 'ASIS_MED', 'N_DIR1', 'T_DIR1', 'M_DIR1', 'C_DIR1', 'C_DIR12', 'N_ANT1', 'T_ANT1',
                      'M_ANT1', 'C_ANT1', 'C_ANT12', 'N_ANT2', 'T_ANT2', 'M_ANT2', 'C_ANT2', 'C_ANT22', 'N_ANT3',
                      'T_ANT3',
                      'M_ANT3', 'C_ANT3', 'C_ANT32', 'N_PAT1', 'T_PAT1', 'M_PAT1', 'C_PAT1', 'N_PAT2', 'C_PAT2',
                      'N_BAS1',
                      'C_BAS1', 'N_MCM1', 'C_MCM1', 'CAUSA_666', 'IDPROFCER', 'DD_EXP', 'MM_EXP', 'FECHA_EXP',
                      'FECHAGRA',
                      'CAU_HOMOL', 'GRU_ED1', 'GRU_ED2', 'HORA_SE']
        # Dataframe de los datos del csv relacionados con la mortalidad de una enfermedad
        self.dataframe = pd.read_csv(self.ruta, sep=self.sep, header=None, names=self.names)
        # Dataframe con los codigos de los municipios y departamentos del pais
        self.dataframe_lugar = pd.read_csv("DATASETS/Departamentos_y_municipios_de_Colombia.csv", sep=self.sep,
                                           header=None,
                                           names=['REGION', 'CÓDIGO DANE DEL DEPARTAMENTO', 'DEPARTAMENTO',
                                                  'CÓDIGO DANE DEL MUNICIPIO', 'MUNICIPIO'])
        # Tupla con los nombres de los meses en español
        self.meses = (
            "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre",
            "Noviembre",
            "Diciembre")
        self.enfermedad = enfermedad

    # Metodo para la verificación de valores vacios extraidos del dataframe
    def isNaN(self, num):
        return num != num

    # Metodo para el llenado de digitos de los numeros
    def llenar(self, param):
        return str(('0' * (3 - len(str(param)))) + str(param))

    # Metodo encargado de la extraccion del nombre de un departamento con base en su codigo
    def search_departament(self, clave):
        for i in self.dataframe_lugar.index:
            if self.dataframe_lugar['CÓDIGO DANE DEL DEPARTAMENTO'][i] == clave:
                return self.dataframe_lugar['DEPARTAMENTO'][i]
        else:
            return "SIN NOMBRE"

    # Metodo encargado de la extraccion de la fecha de nacimiento de una persona, permitiendo la limpieza
    # de los valores y haciendo uso de los valores de fecha de defuncion y edad en los casos requeridos
    # en donde no se tenga la fecha de nacimiento de la persona, además de arreglar problemas de digitación
    def fecha_nam(self, i):
        clave, fechad, edad = self.dataframe['FECHA_NAC'][i], self.dataframe['FECHA_DEF'][i], self.dataframe['EDAD'][i]
        if type(clave) is str:
            if not self.isNaN(clave):
                dt2 = parse(fechad)
                return dt2 + relativedelta(years=-int(edad))
            else:
                dt, dt2 = parse(clave), parse(fechad)
                if dt > dt2:
                    dt = dt + relativedelta(years=-100)
                return dt.date()
        else:
            dt2 = parse(fechad)
            return dt2 + relativedelta(years=-int(edad))

    # Metodo encargado de la extraccion del nombre de un municipio con base en su codigo
    def search_municipe(self, clave):
        for i in self.dataframe_lugar.index:
            if self.dataframe_lugar['CÓDIGO DANE DEL MUNICIPIO'][i] == clave:
                return self.dataframe_lugar['MUNICIPIO'][i]
        else:
            return "SIN NOMBRE"

    # Metodo para la extraccion de los datos de los departamentos en donde ocurriendo las defunciones
    # en donde se crean tuplas con el codigo y nombre del departamento, para ser almacenados en
    # la lista. Se controlan los duplicados con una lista auxiliar en donde se almacenen los
    # codigos de los departamentos
    def extract_departament(self):
        lista, pk = [], []
        for i in self.dataframe.index:
            clave = self.dataframe['COD_DPTO'][i]
            if clave not in pk:
                pk.append(clave)
                aux = (clave, self.search_departament(clave))
                lista.append(aux)
        return lista

    # Metodo para la extraccion de los datos de las instituciones en donde ocurriendo las defunciones
    # en donde se crean tuplas con el codigo y nombre de la institucion, para ser almacenados en
    # la lista. Se controlan los duplicados con una lista auxiliar en donde se almacenen los
    # codigos de las intituciones. Esta columna tienen valores vacios, que se cnontrolar con el metodo isNaN
    def extract_institutes(self):
        lista, pk = [], []
        for i in self.dataframe.index:
            if not self.isNaN(self.dataframe['COD_INST'][i]):
                clave = str(self.dataframe['COD_INST'][i]).replace(';', ".")
                if clave not in pk:
                    pk.append(clave)
                    aux = (clave, self.dataframe['NOM_INST'][i])
                    lista.append(aux)
        return lista

    # Metodo para la extraccion de los datos de los departamentos en donde ocurriendo las defunciones
    # en donde se crean tuplas con el codigo del municipio, nombre del municipio y codigo del departamento, para ser almacenados en
    # la lista. Se controlan los duplicados con una lista auxiliar en donde se almacenen los una clave compuesta por el codigo
    # del departamento y del municipio
    def extract_municipes(self):
        lista, pk = [], []
        for i in self.dataframe.index:
            clave = int(str(self.dataframe['COD_DPTO'][i]) + (self.llenar(self.dataframe['COD_MUNIC'][i])))
            if clave not in pk:
                pk.append(clave)
                aux = (self.dataframe['COD_MUNIC'][i], self.dataframe['COD_DPTO'][i], self.search_municipe(clave))
                lista.append(aux)
        return lista

    # Metodo para la extraccion de los lugares de defuncion de las personas, en donde se crean tuplas con una clave compuesta
    # por el codigo del dep, municipio, y si ocurrio en una institucion, esta se añade. Se realiza un control del valor
    # vacio de la institucion y de los egistros duplicados por medio de la clave compuesta.
    def extract_place(self):
        lista, pk, ins = [], [], ""
        for i in self.dataframe.index:
            if not self.isNaN(self.dataframe['COD_INST'][i]):
                ins = str(self.dataframe['COD_INST'][i]).replace(';', ".")
                clave = str(self.dataframe['COD_DPTO'][i]) + "-" + str(self.dataframe['COD_MUNIC'][i]) + "-" + ins
            else:
                ins = None
                clave = str(self.dataframe['COD_DPTO'][i]) + "-" + str(self.dataframe['COD_MUNIC'][i])
            if clave not in pk:
                pk.append(clave)
                aux = (
                    clave, self.dataframe['SIT_DEFUN'][i], self.dataframe['COD_DPTO'][i],
                    self.dataframe['COD_MUNIC'][i],
                    ins)
                lista.append(aux)
        return lista

    # Metodo para la extracion de las fechas de las defunciones registradas, en donde se crean tuplas con la fecha completa,
    # el año, nombre del mes y día, se controlan los valores duplicados por medio de una lista auxiliar con las fechas
    # completas
    def extract_date(self):
        l, pk = [], []
        for i in self.dataframe.index:
            fechad = self.dataframe['FECHA_DEF'][i]
            dt2 = parse(fechad).date()
            if dt2 not in pk:
                aux = (dt2, dt2.year, self.meses[dt2.month - 1], dt2.day)
                pk.append(dt2)
                l.append(aux)
        return l

    # Extraccion de los datos personas de los fallecidos, creacion tuplas con ña fecha de nacimiento, edad, sexo y seguridad
    # social de la persona, además de una clave compuesta usando todos los datos anteriomente expuestos
    def extract_person(self):
        l, pk = [], []
        for i in self.dataframe.index:
            f, edad, sexo, segsoc = self.fecha_nam(i), self.dataframe['EDAD'][i], self.dataframe['SEXO'][i], \
                                    self.dataframe['SEG_SOCIAL'][i]
            clave = str(f.year) + str(f.month) + str(f.day) + "-" + str(edad) + "-" + sexo[0] + "-" + segsoc
            if clave is not pk:
                pk.append(clave)
                aux = (clave, f, edad, sexo, segsoc)
                l.append(aux)
        return l

    # Metodo encargado de la extraccion de los datos principales del fallecimiento de una persona, contemplando
    # la fecha, creacion de la clave compuesta para el lugar y la persona y la cinluscion del nombre de la enfermedad
    # y el dato de si la persona recibio o no asistencia medica
    def extract_deaths(self, enf):
        lista, pk, lugar = [], [], ""
        for i in self.dataframe.index:
            if not self.isNaN(self.dataframe['COD_INST'][i]):
                ins = str(self.dataframe['COD_INST'][i]).replace(';', ".")
                lugar = str(self.dataframe['COD_DPTO'][i]) + "-" + str(self.dataframe['COD_MUNIC'][i]) + "-" \
                        + ins
            else:
                lugar = str(self.dataframe['COD_DPTO'][i]) + "-" + str(self.dataframe['COD_MUNIC'][i])
            fechad = self.dataframe['FECHA_DEF'][i]
            df = parse(fechad).date()
            f, edad, sexo, segsoc = self.fecha_nam(i), self.dataframe['EDAD'][i], self.dataframe['SEXO'][i], \
                                    self.dataframe['SEG_SOCIAL'][i]
            persona = str(f.year) + str(f.month) + str(f.day) + "-" + str(edad) + "-" + sexo[0] + "-" + segsoc
            am = str(self.dataframe['ASIS_MED'][i]).upper()
            if am == "IG" or am == "SÍ":
                am = "SI"
            aux = (lugar, df, persona, enf, am)
            lista.append(aux)
        return lista


# Clase encargada de las tranformaciones necesarias para la carga de los datos extraidos de la ODS
# para el DW.
class Tranformaciones:

    # Constructora sin parametros
    def __init__(self):
        pass

    # Método encargado de la búsqueda de la posición de un item en una lista de tuplas, de modo tal se comparar las
    # Posiciones de la tupla ingresada con las posiciones de cada una de las tuplas de la lista. Se retorna la posición
    # de la tupla con el numero K de posiciones de la tupla. Se envía -1 si no se encuentra la tupla en la lista
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
    # Creacion de la conexion con ods_bona_health
    ods = MySQLCon('localhost', 3306, 'root', 'password', 'ods_bona_health')
    if ods.condb is not None:
        # Creacion de los dataframes para los csv con los datos de mortalidad
        arc1, arc2, arc3 = ArchivoMort("Mort_Can_Mama.csv", "Cancer de Mama"), ArchivoMort("Mort_Can_Pulmon.csv",
                                                                                           "Cancer de Pulmón"), ArchivoMort(
            "Mort_VIH.csv", "VIH")
        print("INICIO CARGA DE DATOS ODS", datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        # Lista de enfermedades a cargar
        lista = [(arc3.enfermedad, "Virus de inmunodeficiencia humana"), (arc1.enfermedad, ""), (arc2.enfermedad, "")]
        # carga de la tabla enfermedad del ODS
        ods.load("ods_bona_health.enfermedad (nombre,observacion) VALUES (%s, %s)", lista, "ENFERMEDAD")
        # Ciclo for para la extracion de datos de los csv y su porterior carga al ODS
        for i in range(7):
            if i == 0:
                lista = arc1.extract_departament() + arc2.extract_departament() + arc3.extract_departament()
            elif i == 1:
                lista = arc1.extract_institutes() + arc2.extract_institutes() + arc3.extract_institutes()
            elif i == 2:
                lista = arc1.extract_municipes() + arc2.extract_municipes() + arc3.extract_municipes()
            elif i == 3:
                lista = arc1.extract_place() + arc2.extract_place() + arc3.extract_place()
            elif i == 4:
                lista = arc1.extract_date() + arc2.extract_date() + arc3.extract_date()
            elif i == 5:
                lista = arc1.extract_person() + arc2.extract_person() + arc3.extract_person()
            elif i == 6:
                lista = arc1.extract_deaths("Cancer de Mama") + arc2.extract_deaths(
                    "Cancer de Pulmón") + arc3.extract_deaths("VIH")
            # Carga de los datos a la tabla especificada para cada iteración
            ods.load(ods.insertsODS[i][0], lista, ods.insertsODS[i][1])
        print("FIN DE CARGA DE DATOS ODS", datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
        # Creacion de la conexion a bona_health_dw
        bonaheathl_dw = MySQLCon('localhost', 3306, 'root', 'password', 'bona_health_dw')
        if bonaheathl_dw.condb is not None:
            # Instancia de la clase con las tranformaciones
            tranform = Tranformaciones()
            print("INICIO DE CARGA DE DATOS A BONA_HEALTH_DW: ", datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
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
    else:
        print("NO SE PUDO CONECTAR A ", ods.database)
