
import pymongo


class OperacionesDb:

    # Colecciones
    SIMU_DATA = "simu_data"
    INFRASTRUCTURE = "infrastructure"

    IP = "127.0.0.1"  # 192.168.1.69  # tarro
    PUERTO = "27017"
    MY_EXECUTION_ID = 0.0

    def __init__(self):
        pass

    @classmethod
    def generar_conn(cls, _db, _colecction):
        mongo_uri = "mongodb://" + cls.IP + ":" + cls.PUERTO + "/"
        myclient = pymongo.MongoClient(mongo_uri, connect=False)
        mydb = myclient[_db]
        mycol = mydb[_colecction]
        return myclient, mycol

    @classmethod
    def guardar_solucion(cls, _solucion, app_id, componentes):
        """ se guarda la solucion [contexto][app_id_componentes]"""
        try:
            cls.NUM_SOLUCION += 1
            _myclient, _mycol = cls.generar_conn(cls._db, cls.SOLUCIONES)

            if _solucion == None:
                input(" _solucion es None, revisar")

            _solucion_json = _solucion.toJSON()

            combinacion = str(_solucion.combination)
            combinacion = combinacion.replace(" ", "")

            _key = cls.create_deploy_instance_key(app_id, componentes)

            aux_utilizaciones = []
            for pm_vm_id, comps_acts_id in _solucion.util_detallado.items():
                for comp_act_id, list_utils in comps_acts_id.items():
                    act_id = comp_act_id.split("#")
                    act_id = int(act_id[1])

                    aux_util = {"app_componentes_hashed": cls._key, "combinacion": combinacion,
                                "metrica": cls.UTIL_DETALLE, "valor_metrica": list_utils,
                                "descripcion": "", "pm_vm_id": pm_vm_id, "comp_act_id": comp_act_id, "act_id": act_id,
                                "execution_id": cls.MY_EXECUTION_ID, "NUM_SOLUCION": cls.NUM_SOLUCION}
                    aux_utilizaciones.append(aux_util)
                    #if cls.NUM_SOLUCION == 11:
                    #    print()

            _mycol.insert_many(aux_utilizaciones)  # por si hay muchas actividades

            aux_tiempos = []
            for k1, v1 in _solucion.tiempos_medios_respuesta_final_erlang.items():
                for k2, v2 in v1.items():
                    for k3, v3 in v2.items():
                        for k4, v4 in v3.items():
                            for descripcion_funcion, tiempos in v4.items():
                                #for tiempo in tiempos:
                                    #_mycol.insert_one({"app_componentes_hashed": cls._key, "combinacion": combinacion,
                                    #"metrica": cls.TIEMPOS_ERLANG, "valor_metrica": tiempo, "posicion": i,
                                    #"descripcion": descripcion_funcion, "k2": k2, "k3": k3, "k4": k4})
                                    # TODO ver como obtengo la act_id, que es k1 2 3 4
                                    aux_tiempo = {"app_componentes_hashed": cls._key, "combinacion": combinacion,
                                                  "metrica": cls.TIEMPOS_ERLANG, "valor_metrica": tiempos,
                                                  "descripcion": descripcion_funcion, "k2": k2, "k3": k3, "k4": k4,
                                                  "act_id": -1, "execution_id": cls.MY_EXECUTION_ID,
                                                  "NUM_SOLUCION": cls.NUM_SOLUCION}
                                    aux_tiempos.append(copy.deepcopy(aux_tiempo))
            _mycol.insert_many(aux_tiempos)  # por si hay muchas actividades

            id_comp = componentes[0].id
            otros_parametros_y_metricas = {"app_componentes_hashed": cls._key, "combinacion": combinacion,
                                           "metrica": cls.OTROS, 'ranking': _solucion.ranking,
                                           'total_request_rate': _solucion.total_request_rate[id_comp],
                                          'request_rate_per_host_group': _solucion.distributed_request_rate,
                                          'detalle_porcentajes': _solucion.detalle_porcentajes, "NUM_SOLUCION": cls.NUM_SOLUCION }
            _mycol.insert_one(otros_parametros_y_metricas)

            _myclient.close()

            # _mycol.insert_one({"_id": cls._key, "combinacion": combinacion})
            #print()

        except Exception as e:
            print(
                type(e).__name__,          # TypeError
                __file__,                  # /tmp/example.py
                e.__traceback__.tb_lineno  # 2
            )
            print()
        pass

    @classmethod
    def set_context_key(cls, zones, method_params):

        zones_str, method_params_str = cls.zones_method_params_to_string(zones, method_params)
        _key = zones_str + method_params_str
        _key = hashlib.md5(_key.encode())

        _key = _key.hexdigest()
        cls._key = _key

    @classmethod
    def check_if_combination_is_calculated(cls, combinacion, componentes, app_id):
        """ Componentes del deploy (?)"""
        componentes = str(componentes)
        componentes = componentes.replace(" ", "")
        _key = str(app_id) + "_" + componentes
        try:
            if _key not in cls.soluciones["apps"]:
                return False
            combinacion = str(combinacion)
            combinacion = combinacion.replace(" ", "")
            if combinacion in cls.soluciones["apps"][_key]:
                return True
            else:
                return False
        except Exception as e:
            print()

    @classmethod
    def create_deploy_instance_key(cls, app_id, componentes):
        ids = []
        for comp in componentes:
            ids.append(comp.id)
        ids = str(ids)
        ids = ids.replace(" ", "")
        _key = str(app_id) + "_" + ids
        return _key

    @classmethod
    def db_cargar_soluciones(cls, app_id, componentes):

        componentes = str(componentes)
        componentes = componentes.replace(" ", "")
        _key = str(app_id) + "_" + componentes

        #app_componentes_hashed = cls._key
        #execution_id = 1647244560.20498

        try:
            _myclient, _mycol = cls.generar_conn(cls._db, cls.SOLUCIONES)
            soluciones = {"_key": cls._key, "soluciones": {}}
            for doc in _mycol.find({"app_componentes_hashed": cls._key}):
                #soluciones = doc["apps"][_key]["soluciones"]

                combinacion = doc['combinacion']
                if combinacion not in soluciones["soluciones"]:
                    soluciones["soluciones"][combinacion] = {'utilizaciones_detalle': 0, "tiempos_erlang": 0}
                soluciones["soluciones"][combinacion][doc["metrica"]] = doc
            return soluciones
        except Exception as e:
            print()

    @classmethod
    def guardar_tiempo_ejecucion(cls, tiempo_total, medida):
        _myclient, _mycol = cls.generar_conn(cls._db, cls.TIEMPO_EJECUCION)

        x_json = {"key_contexto": cls._key, "tiempo_ejecucion": tiempo_total, "execution_id": cls.MY_EXECUTION_ID,
                  "medida": medida}
        try:
            res = _mycol.insert_one(x_json)
        except Exception as e:
            print()
        return

    @classmethod
    def guardar_solucion_elegida(cls, combinacion: [int], app_id: int, dep_inst_componentes: [int]):
        _myclient, _mycol = cls.generar_conn(cls._db, cls.APLICACIONES)
        key_app_comps = str(app_id) + "_" + str(dep_inst_componentes)
        x_json = {"key_contexto": cls._key, "app_componentes": key_app_comps, "combinacion": str(combinacion),
                  "execution_id": cls.MY_EXECUTION_ID}
        cls.CONTEXT_PARAMS["app_id"] = app_id
        cls.CONTEXT_PARAMS["solucion"] = x_json
        try:
            res = _mycol.insert_one(x_json)
        except Exception as e:
            print()
        return
