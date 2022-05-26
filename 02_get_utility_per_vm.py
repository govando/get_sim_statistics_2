import ast
import json
import statistics
from collections import defaultdict

import numpy as np
import pymongo
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt

from db_operations import OperacionesDb

# Colecciones
SIMU_DATA = "simu_data"
INFRASTRUCTURE = "infrastructure"
_db = "simulation_platform"
method_mean_time_response = 4.687263811616415  # tiempo segun metodologia
method_system_mean_utility = 0.5  # utilizacion
saved_dataset = []
dataset_filename = "dataset_filename.data"


def main():
    #data = open_file(dataset_filename)
    #if data == None:
    dataset = get_dataset_from_db()
    #else:
    #    dataset = get_dataset_from_file(data)
    dataset_statistics, errors = get_statistics(dataset, method_mean_time_response)
    print("\n")
    print(dataset_statistics)
    print(errors)
    print()


def get_dataset_from_file(data):
    # Converting string to list
    res = ast.literal_eval(data)
    return res


def get_statistics(dataset, scalar_metric):
    mean = get_mean(dataset)
    geometric_mean = get_geometric_mean(dataset)
    median = get_median(dataset)
    variance = get_variance(dataset)
    RE = get_relative_error_by_root_mean_square_error(dataset, scalar_metric)
    dataset_wo_outliers = remove_outliers_iqr(dataset)
    RE_sin_outliers = get_relative_error_by_root_mean_square_error(dataset_wo_outliers, scalar_metric)
    mean_error = get_mean_error(mean, scalar_metric)


    return [mean, geometric_mean, median, variance], [RE,RE_sin_outliers, mean_error]


def remove_outliers_iqr(data):
    #  Remuevo outliers con IQR
    sorted(data)
    q1, q3 = np.percentile(data, [25, 75])
    iqr = q3 - q1
    lower_bound = q1 - (1.5 * iqr)
    upper_bound = q3 + (1.5 * iqr)
    dataSinOutlier = []
    for numero in data:
        if lower_bound <= numero <= upper_bound:
            dataSinOutlier.append(numero)
    return dataSinOutlier

def get_dataset_from_db():
    myclient, mycol = OperacionesDb.generar_conn(_db, SIMU_DATA)


    agg_2_0 = {"$group": {
        "_id": { "pgmdDeployment": "$pgmdDeployment"},
        "count": {"$sum": 1}
    }}

    vm_core_data = defaultdict(lambda: defaultdict(lambda: []))
    vm_utilization_per_core = defaultdict(lambda: defaultdict(lambda: []))
    vm_average_util = defaultdict(lambda: 0.0)
    id_pgm = -1
    #while True:
    for id_pgm in range(0, 5):
        #id_pgm += 1
        agg_1_0 ={
            "$match":{     "idSimulation":"ori-5d129f453cf6b24dd54750d2",
                           "collection":"simu_data",
                           "type":"simPrograms",
                           "nbrProcessMsgExecuted":1,
                           "idpgm": id_pgm}
        }

        vms = mycol.aggregate([ agg_1_0, agg_2_0])
        #flag_break = True
        for doc in vms:
            #flag_break = False
            ip_port: str = doc["_id"]["pgmdDeployment"]#["ip"]
            ip:int = json.loads(ip_port)["ip"]
            vm_datas = mycol.find(
                {"idSimulation":"ori-5d129f453cf6b24dd54750d2",
                 "collection":"simu_data",
                 "type":"simVirtualmachines", "ip":ip})
            for vm_data in vm_datas:
                cores_names = vm_data["coresName"]
            # recolectamos datos de cada core
            for core_name in cores_names:
                min_core_timestamp = 999999999
                max_core_timestamp = 0
                core_datas = mycol.find(
                    {"idSimulation":"ori-5d129f453cf6b24dd54750d2",
                     "collection":"simu_data",
                     "type":"simCores", "nbrProcessMsgExecuted":1,
                     "idpgm":id_pgm, "coreName": core_name
                     }).sort("msCurrentTime", 1)
                for core_data in core_datas:
                    if max_core_timestamp < core_data["msCurrentTime"]:
                        max_core_timestamp = core_data["msCurrentTime"]
                    if min_core_timestamp > core_data["msCurrentTime"]:
                        min_core_timestamp = core_data["msCurrentTime"]

                    vm_core_data[ip][core_name].append(core_data["msExecTime"])
                total_used_time = sum(vm_core_data[ip][core_name])
                #vm_utilization_per_core[ip][core_name] = total_used_time/(max_core_timestamp - min_core_timestamp)
                vm_utilization_per_core[ip][core_name] = total_used_time/495
        #if flag_break:
        #    break
    total_utility = 0.0
    for ip, core_data in vm_utilization_per_core.items():
        for core_name, core_util in vm_utilization_per_core[ip].items():
            total_utility += core_util
        vm_average_util[ip] = total_utility / len(vm_utilization_per_core[ip])
    print("a")

    cursor_all_pgm_msgs = mycol.find(
        {"idSimulation": "ori-5d129f453cf6b24dd54750d2",
         "collection": "simu_data",
         "type": "simPrograms",
         "nbrProcessMsgExecuted":1})
    list_all_pgm_msgs = []
    for doc in cursor_all_pgm_msgs:
        list_all_pgm_msgs.append(doc)

    cursor_all_network_msgs = mycol.find(
        {"idSimulation":"ori-5d129f453cf6b24dd54750d2",
         "collection":"simu_data",
         "type":"simNetwork",
         "nbrMsgTrasmitted":1})
    list_all_network_msgs = []
    for doc in cursor_all_network_msgs:
        list_all_network_msgs.append(doc)

    time_msgs = {}
    for msge in sorted_original_msgs:
        root_id_msg = msge['idmsg']
        time_msgs[root_id_msg] = 0.0
        for i, msg_pgm in enumerate(list(list_all_pgm_msgs)):

            flag = check_relation_msg_id(root_id_msg, msg_pgm)
            if flag:
                time_msgs[root_id_msg] += msg_pgm['msExecTime']
                #del list_all_pgm_msgs[i]
        for i, msg_pgm in enumerate(list(list_all_network_msgs)):
            flag = check_relation_msg_id(root_id_msg, msg_pgm)
            if flag:
                time_msgs[root_id_msg] += msg_pgm['timeByMsg']
                #del list_all_network_msgs[i]
        advance_print()
        #all_pgm_msgs.rewind()
        #all_network_msgs.rewind()
    dataset = list(time_msgs.values())
    write_file(dataset_filename, dataset)
    return dataset

def get_geometric_mean(dataset):
    gmean = 1
    for item in dataset:
         gmean *= item
    gmean **= 1 / len(dataset)
    return gmean


def get_mean(dataset):
    return sum(dataset) / len(dataset)


def get_median(dataset):
    n = len(dataset)
    if n % 2:
         median_ = sorted(dataset)[round(0.5*(n-1))]
    else:
        x_ord, index = sorted(dataset), round(0.5 * n)
        median_ = 0.5 * (x_ord[index-1] + x_ord[index])
    return median_


def get_relative_error_by_root_mean_square_error(dataset, scalar_value):
    """ recibe un dataset y un valor escalar que se transforma a un dataset"""
    #new_dataset_lenght = int(len(dataset)*.1)
    #dataset = dataset[:new_dataset_lenght]
    # outliers removed a lo indio
    #dataset = [x for x in dataset if x > 2.5]

    scalar_value_dataset = [scalar_value] * len(dataset)
    error_cuadratico_medio = mean_squared_error(dataset, scalar_value_dataset, squared=False)
    error_relativo = error_cuadratico_medio / method_mean_time_response
    # plotear
    datasets = [dataset, scalar_value_dataset]
    labels = ["dataset", "scalar_value_dataset"]
    plot_data(datasets, labels, "mean time response sim vs method")
    return error_relativo


def get_mean_error(dataset_mean, scalar_value):
    mean_error = dataset_mean / scalar_value
    if dataset_mean <= scalar_value:
        mean_error = 1 - mean_error
    else:
        mean_error = mean_error - 1
    return mean_error


def get_variance(dataset):
    return statistics.variance(dataset)


def check_relation_msg_id(root_id_msg: str, msg_pgm) -> bool:
    msg_id: str = msg_pgm['idmsg']
    msg_id_parts:[str] = msg_id.split("_")
    aux_root_id = msg_id_parts[0] + "_" + msg_id_parts[1]
    return  root_id_msg == aux_root_id

total:int = 180
adv_aux: int = 0


def advance_print():
    global adv_aux, total
    adv_aux += 1
    print("-", end="")
    if adv_aux % total == 0:
        print("\n")


def write_file(filename, content):
    f = open(filename, "w")
    f.write(str(content))
    f.close()


def open_file(filename):
    try:
        f = open(filename, "r")
        return f.read()
    except:
        return None


def plot_data(datasets, labels, titulo="", ruta=""):
    for i, dataset in enumerate(datasets):
        plt.plot(dataset, label=labels[i])
    #path_titulo = caso_tramo + " " + componente

    plt.title(titulo)
    plt.legend()
    #ruta_completa = pathfile + nombre_carpeta + path_titulo + ".png"
    #plt.savefig(ruta_completa)
    plt.show()

if __name__ == '__main__':
    main()