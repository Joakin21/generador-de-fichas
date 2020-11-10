import random
import names
import barnum
import constant
import json
from pymongo import MongoClient
from bson import ObjectId

def randomDate():
    return str(barnum.create_date(past=True)).split()[0]

def addField(field_data):
    type_field_data = field_data['tipo']

    field_obj = {}
    field_obj["clave"] = field_data["text"]
    field_obj["tipo"] = 4

    if (type_field_data == "estructural"):
        field_obj["valor"] = field_data["text"]
        field_obj["tipo"] = 2

    elif (type_field_data == "CLUSTER"):
        field_obj["valor"] = field_data["text"]
        field_obj["tipo"] = 3

    elif ((type_field_data == "DV_CODED_TEXT" or type_field_data == "CHOICE" or type_field_data == "DV_ORDINAL") and field_data["contenido"]):
        field_obj["valor"] = random.choice(field_data["contenido"])["text"]

    elif (type_field_data == "DV_QUANTITY" or type_field_data == "DV_COUNT"):
        field_obj["valor"] = random.randint(1,100)

    elif (type_field_data == "DV_DATE_TIME" or type_field_data == "DV_DATE"):
        field_obj["valor"] = randomDate()

    elif (type_field_data == "DV_BOOLEAN"):
        field_obj["valor"] = random.choice(["Yes", "No"])

    else:
        field_obj["valor"] = barnum.create_sentence()

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

client = MongoClient()
db = client['proyecto4']
archetype_collection = db["arquetipos"]  

n_collections = archetype_collection.count_documents({})
archetype_list = archetype_collection.find({}, {"text":1})

health_records = []

for ehr in range(constant.N_HEALTH_RECORDS):
    patient_history = {}
    patient_history["_id"] = {"$oid":str(ObjectId())}
    patient_history["nombre"] = names.get_first_name()
    patient_history["apellidos"] = names.get_last_name() + " " + names.get_last_name()
    patient_history["rut"] = str( random.randint(11111111,99999999) ) + '-' + str( random.randint(1,9) )
    patient_history["direccion"] = barnum.create_street()
    patient_history["fecha_nacimiento"] = randomDate()
    patient_history["ciudad"] = random.choice(constant.CITIES)
    patient_history["profesionales_que_atendieron"] = random.sample(constant.HC_PROFESSIONAL_ID, random.randint(1,len(constant.HC_PROFESSIONAL_ID)))#len(constant.HC_PROFESSIONAL_ID)
    patient_history["sesiones_medica"] = []

    n_sessions = len(patient_history["profesionales_que_atendieron"])
    for i in range(n_sessions):#random.randint(1,8)
        medical_session = {}
        medical_session["nombre_sesion"] = barnum.create_nouns()
        medical_session["fecha"] = randomDate()
        medical_session["nombre_profesional"] = names.get_first_name() + " " + names.get_last_name() + " " + names.get_last_name()
        medical_session["profesion"] = random.choice(constant.HC_PROFESSIONAL_PRACTITIONERS)
        medical_session["centro_salud"] = random.choice(constant.HOSPITALS)
        medical_session["user_id"] = random.choice(patient_history["profesionales_que_atendieron"])
        medical_session["arquetipos"] = []
        patient_history["sesiones_medica"].append(medical_session)
        n_archetype = random.randint(1,5)
        for i in range(n_archetype):
            
            archetype_from_db = archetype_collection.find_one({"text":archetype_list[random.randint(0,n_collections-1)]['text']})
            archetype_from_db.pop('_id')
            archetype_for_session = [{
                "tipo":1, "clave":archetype_from_db['text'], "valor":archetype_from_db['text']
            }]

            iterateArchetype(archetype_from_db)
            medical_session["arquetipos"].append(archetype_for_session)
    
    health_records.append(patient_history)

with open('health_records.json', 'w') as json_file:
    json.dump(health_records, json_file)