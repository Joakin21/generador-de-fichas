import random
import names
import barnum
import constant
import json
from pymongo import MongoClient
from bson import ObjectId
from cryptography.fernet import Fernet

def randomDate():
    return str(barnum.create_date(past=True)).split()[0]

def addField(field_data):
    #, option
    type_field_data = field_data['tipo']
    
    field_obj = {}
    field_obj["clave"] = process_data(field_data["text"], option)
    field_obj["tipo"] = process_data("4", option)

    if (type_field_data == "estructural"):
        field_obj["valor"] = process_data(field_data["text"], option)
        field_obj["tipo"] = process_data("2", option)

    elif (type_field_data == "CLUSTER"):
        field_obj["valor"] = process_data(field_data["text"], option)
        field_obj["tipo"] = process_data("3", option)

    elif ((type_field_data == "DV_CODED_TEXT" or type_field_data == "CHOICE" or type_field_data == "DV_ORDINAL") and field_data["contenido"]):
        field_obj["valor"] = process_data(random.choice(field_data["contenido"])["text"], option)

    elif (type_field_data == "DV_QUANTITY" or type_field_data == "DV_COUNT"):
        field_obj["valor"] = process_data(str(random.randint(1,100)), option)

    elif (type_field_data == "DV_DATE_TIME" or type_field_data == "DV_DATE"):
        field_obj["valor"] = process_data(randomDate(), option)

    elif (type_field_data == "DV_BOOLEAN"):
        field_obj["valor"] = process_data(random.choice(["Yes", "No"]), option)

    else:
        field_obj["valor"] = process_data(barnum.create_sentence(), option)

    archetype_for_session.append(field_obj)

def iterateArchetype(my_archetype):
    for k in my_archetype:
        
        if (isinstance(my_archetype[k], dict) and my_archetype[k] != None  and "tipo" in my_archetype[k]):

            if (my_archetype[k]['tipo'] != "estructural" and my_archetype[k]['tipo'] != "info" and my_archetype[k]['text']):
                addField(my_archetype[k])

            elif (my_archetype[k]["tipo"] == "estructural"):
                es_nodo_info = False
                for nodo_siguiente in my_archetype[k]:
                    if (isinstance(my_archetype[k][nodo_siguiente], dict) and my_archetype[k][nodo_siguiente] != None and "tipo" in my_archetype[k][nodo_siguiente]):
                        if (my_archetype[k][nodo_siguiente]['tipo'] == "info"):
                            es_nodo_info = True
                if (not es_nodo_info):
                    addField(my_archetype[k])

            iterateArchetype(my_archetype[k])

    
def process_data(data, option):
    if option == 1:
        return data
    elif option == 2:
        return algoritmo.encrypt(data.encode("utf-8")) 

client = MongoClient()
db = client['proyecto4']
archetype_collection = db["arquetipos"]  
paciente_collection = db["historial_paciente"]

n_collections = archetype_collection.count_documents({})
archetype_list = archetype_collection.find({}, {"text":1})

key_test = "QJhfUw_LWLPe6uEbDd808C9eUeOxUBQfj5a4ln6o8UU=".encode()
algoritmo = Fernet(key_test)

health_records = []
n_health_records = int(input("Ingrese el número de fichas a crear: "))

print("Seleccione una opción:")
print("1. Generar archivo json")
print("2. insertar en base de datos mongo")
option = int(input("Opción: "))


for ehr in range(n_health_records):
    patient_history = {}
    if option == 1:
        patient_history["_id"] = {"$oid":str(ObjectId())}

    patient_history["nombre"] = process_data(names.get_first_name(), option)
    patient_history["apellidos"] = process_data(names.get_last_name() + " " + names.get_last_name(), option)
    patient_history["rut"] = str( random.randint(11111111,99999999) ) + '-' + str( random.randint(1,9) )
    patient_history["direccion"] = process_data(barnum.create_street(), option)
    patient_history["fecha_nacimiento"] = process_data(randomDate(), option)
    patient_history["ciudad"] = process_data(random.choice(constant.CITIES), option)
    patient_history["es_atendido_ahora"] = False
    """
    if option == 1:
        patient_history["profesionales_que_atendieron"] = random.sample(constant.HC_PROFESSIONAL_ID, random.randint(1,len(constant.HC_PROFESSIONAL_ID)))#len(constant.HC_PROFESSIONAL_ID)
        n_sessions = len(patient_history["profesionales_que_atendieron"])
    if option == 2:
        patient_history["profesionales_que_atendieron"] = [{"user_id":random.choice([117, 118]), "fecha":"07/02/2021"}]
        n_sessions = random.randint(1,15)
    """
    n_sessions = random.randint(1,15)
    patient_history["sesiones_medica"] = []

    
    for i in range(n_sessions):#random.randint(1,8)
        medical_session = {}
        medical_session["nombre_sesion"] = process_data(barnum.create_nouns(), option)
        medical_session["fecha"] = process_data(randomDate(), option)
        medical_session["nombre_profesional"] = process_data(names.get_first_name() + " " + names.get_last_name() + " " + names.get_last_name(), option)
        medical_session["profesion"] = process_data(random.choice(constant.HC_PROFESSIONAL_PRACTITIONERS), option)
        medical_session["centro_salud"] = process_data(random.choice(constant.HOSPITALS), option)
        medical_session["user_id"] = process_data(str(random.choice(constant.HC_PROFESSIONAL_ID)), option)
        medical_session["arquetipos"] = []
        patient_history["sesiones_medica"].append(medical_session)
        n_archetype = random.randint(1,5)
        for i in range(n_archetype):
            
            archetype_from_db = archetype_collection.find_one({"text":archetype_list[random.randint(0,n_collections-1)]['text']})
            archetype_from_db.pop('_id')
            archetype_for_session = [{
                "tipo":process_data("1", option), "clave":process_data(archetype_from_db['text'], option), "valor":process_data(archetype_from_db['text'], option)
            }]

            iterateArchetype(archetype_from_db)
            medical_session["arquetipos"].append(archetype_for_session)
    
    if option == 1:
        health_records.append(patient_history)
    elif option == 2:
        result_post = paciente_collection.insert_one(patient_history).inserted_id
    print(str(ehr + 1) + "/" + str(n_health_records))

if option == 1:
    with open('health_records.json', 'w') as json_file:
        json.dump(health_records, json_file)