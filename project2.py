from flask import Flask
from flask import jsonify
from flask import request
from flask import Response
from pymongo import MongoClient
import pymongo
from bson.json_util import dumps
from bson import ObjectId
import json
import dateutil.parser
import datetime


#app.config['MONGO_DBNAME'] = 'NoSQL-311CI-BACKUP'
#app.config['MONGO_URI'] = 'mongodb://127.0.0.1:27017/NoSQL-311CI-BACKUP'

#mongo = pymongo(app)

client = MongoClient('localhost:27017')
db = client['NoSQL-311CI-BACKUP']

app = Flask(__name__)

@app.route('/get_last_incident', methods=['GET'])
def get_last_request():
#    data = db.data.find_one({$query{{},{"upvotes":0}},$orderby:{"Creation_Date":-1}})
    data = db.data.find({},{"upvotes":0}).sort([("Creation_Date",-1)]).limit(1) 
    res = data.next()
    res['Creation_Date']=res['Creation_Date'].isoformat()
    if(res['Completion_Date']is not None):
        res['Completion_Date']=res['Completion_Date'].isoformat() 

    if data:
        return dumps({'result':res})
    else:
        return dumps({'result':'Not found!'})


@app.route('/total_per_type_time_range',methods=['GET'])
def query1():
    try:
        reqjson=request.get_json()
        print(reqjson)
        start_date=dateutil.parser.parse(reqjson['start_date'])
        print(start_date)
        end_date=dateutil.parser.parse(reqjson['end_date'])
        print(end_date)
        pipeline = [
                	{ "$match": {"Creation_Date":{"$gte":start_date, "$lte": end_date}} },
                        { "$group": {"_id": "$Type_of_Service_Request", "total": { "$sum": 1 }}},
                        { "$sort" : {"total":-1}}               
                   ]
        data = db.data.aggregate(pipeline)
        return dumps({"Total Request per Type within a specified time range":data}),200,{'Content-Type': 'application/json; charset=utf-8'}
    except:
        res={'Error':'You probably entered wrong data!',"Help":"Enter any date in any format like YYYY-MM-DD HH:MM:SS"}
        return jsonify(res),400,{'Content-Type': 'application/json; charset=utf-8'}


all_types_of_request=['Graffiti Removal','Street Light - 1/Out','Pothole in Street','Garbage Cart Black Maintenance/Replacement','Rodent Baiting/Rat Complaint','Tree Trim','Abandoned Vehicle Complaint','Alley Light Out','Sanitation Code Violation','Tree Debris','Street Lights - All/Out']

@app.route('/number_of_request_for_type_and_time_range',methods=['GET'])
def query2():
    try:
        reqjson=request.get_json()
        print(reqjson)
        start_date=dateutil.parser.parse(reqjson['start_date'])
        print(start_date)
        end_date=dateutil.parser.parse(reqjson['end_date'])
        print(end_date)
        type_of_request=reqjson['type_of_request']
        if type_of_request not in all_types_of_request:
            res={'Error':'You entered an incorrect type of request!',"Help":"Please enter one of the following posible types.","Types of Request":all_types_of_request}
            return jsonify(res),400
        else:
            pipeline=[
                     	{ "$match": { "$and": [{"Creation_Date":{"$gte":start_date, "$lte": end_date}},{"Type_of_Service_Request":type_of_request}]}},
                        { "$group": {"_id": {"$dateToString":{"format":"%Y-%m-%d","date":"$Creation_Date"}},"count": { "$sum": 1 }}}
                     ]
            data = db.data.aggregate(pipeline)

            return dumps({"Total requests for "+type_of_request+". Time range:  "+reqjson['start_date']+" to "+reqjson['end_date']:data}),200,{'Content-Type': 'application/json; charset=utf-8'}
    except:
        res={'Error':'You probably entered wrong data!',"Help":"Enter any date in any format like YYYY-MM-DD HH:MM:SS"}
        return jsonify(res),400,{'Content-Type': 'application/json; charset=utf-8'}



@app.route('/vote',methods=['POST'])
def vote():
    try:
        reqjson=request.get_json()
        name=reqjson['name']
        surname=reqjson['surname']
        address=reqjson['address']
        phone=reqjson['telephone']
        voteid=reqjson['vote_id']

        if db.data.find({"_id":ObjectId(voteid)}).count()>0:
            citizen_doc = db.citizen.find_one({"name":name,"surname":surname,"telephone":phone})
            if citizen_doc:
                print(ObjectId(voteid))
                print(citizen_doc["votes"])
                if ObjectId(voteid) in citizen_doc["votes"]:
                    return dumps({"Error":"Already upvoted "+voteid+" id"}),409,{'Content-Type': 'application/json; charset=utf-8'}
                else:
                    db.data.update({'_id':ObjectId(voteid)},{'$push':{'upvotes':citizen_doc['_id']}})
                    db.citizen.update({'_id':citizen_doc['_id']},{'$push':{'votes':ObjectId(voteid)}})
                    return dumps({"Upvoted":voteid}),200,{'Content-Type': 'application/json; charset=utf-8'}
                return dumps(citizen_doc),200,{'Content-Type': 'application/json; charset=utf-8'}
            else:
                vt=set()
                vt.add(ObjectId(voteid))
                res = db.citizen.insert_one(
                        {
                            'name':name,
                            'surname':surname,
                            'address':address,
                            'telephone':phone,
                            'votes':list(vt)
                        }
                        )
                print('Created user:',res.inserted_id)
                db.data.update({'_id':ObjectId(voteid)},{'$push':{'upvotes':res.inserted_id}})

                return dumps({"Created citizen":res.inserted_id,"Voted for": ObjectId(voteid)}),200,{'Content-Type': 'application/json; charset=utf-8'}
            
            return dumps({"voteid":"found"}),200,{'Content-Type': 'application/json; charset=utf-8'}
        else:
            return dumps({"voteid":"not found"}),404,{'Content-Type': 'application/json; charset=utf-8'}

    except:
        res=[{'Error':'Something entered wrong!','Help':'Please enter valid vote_id and follow json example.'},{"name":"SpongeBob","surname":"SquarePants","address":"123 Pineaple , 52 Bikini Bottom","telephone":"123-456-789","vote_id":"5c49afcefc3a346ce678e8bf"}]
        return jsonify(res),400,{'Content-Type': 'application/json; charset=utf-8'}


all_status_pos=['Open','Open - Dup','Completed','Completed - Dup']

@app.route('/create_new_request',methods=['POST'])
def create_request():
    try:
        reqjson=request.get_json()
        if reqjson['type_of_request'] not in all_types_of_request:
            res={'Error':'You entered an incorrect type of request!',"Help":"Please enter one of the following posible types.","Types of Request":all_types_of_request}
            return jsonify(res),400
        else:
            if reqjson['Status'] not in all_status_pos:
                res={'Error':'You entered an incorrect status of request!',"Help":"Please enter one of the following status types.","Types of Status":all_status_pos}
                return jsonify(res),400
            else:
                rsfinsert={}
                rsfinsert["Creation_Date"]=datetime.datetime.now()
                rsfinsert["Status"]=reqjson["Status"]
                if reqjson['Status']=='Completed' or reqjson['Status']=='Completed - Dup':
                    rsfinsert["Completion_Date"]= dateutil.parser.parse(reqjson["Completion_Date"])
                else:
                    rsfinsert["Completion_Date"]= None
                rsfinsert["Service_Request_Number"]="testreqnum"
                rsfinsert["Type_of_Service_Request"]=reqjson["type_of_request"]
                rsfinsert["Street_Address"]=reqjson["Street_Address"]
                if reqjson["Zip_Code"]: rsfinsert["Zip_Code"]=int(reqjson["Zip_Code"])
                if reqjson["X_Coordinate"]:rsfinsert["X_Coordinate"]=float(reqjson["X_Coordinate"])
                if reqjson["Y_Coordinate"]:rsfinsert["X_Coordinate"]=float(reqjson["Y_Coordinate"])
                if reqjson["Ward"]:rsfinsert["Ward"]=int(reqjson["Ward"])
                if reqjson["Police_District"]:rsfinsert["Police_District"]=int(reqjson["Police_District"])
                if reqjson["Community_Area"]:rsfinsert["Community_Area"]=int(reqjson["Community_Area"])
                if reqjson["Latitude"]:rsfinsert["Latitude"]=float(reqjson["Latitude"])
                if reqjson["Longitude"]:rsfinsert["Longitude"]=float(reqjson["Longitude"])
                if reqjson["Location"]:rsfinsert["Location"]=reqjson["Location"]
                if reqjson["Latitude"] and reqjson["Longitude"]:rsfinsert["Location_Point"]={"type":"Point","coordinates":[float(reqjson["Longitude"]),float(reqjson["Latitude"])]}
                rsfinsert["upvotes"]=[]
            
                if reqjson["type_of_request"]=="Graffiti Removal":
                    if reqjson["Type_of_Surface"]: rsfinsert["What_Type_of_Surface_is_the_Graffiti_on?"]=reqjson["Type_of_Surface"]
                    if reqjson["Graffiti_location"]:rsfinsert["Where_is_the_Graffiti_located?"]=reqjson["Graffiti_location"]
                    if reqjson["ssa"]:rsfinsert["SSA"]=int(reqjson["ssa"])

#            elif reqjson["type_of_request"]=="Street Light - 1/Out":
                elif reqjson["type_of_request"]=="Pothole in Street":
                    rsfinsert["Current_Activity"]=reqjson["current_activity"]
                    rsfinsert["Most_Recent_Action"]=reqjson["Most_recent_action"]
                    rsfinsert["Number_of_Potholes"]=int(reqjson["Potholes_number"])
                    if reqjson["ssa"]:rsfinsert["SSA"]=int(reqjson["ssa"])

                elif reqjson["type_of_request"]=="Garbage Cart Black Maintenance/Replacement":
                    rsfinsert["Current_Activity"]=reqjson["current_activity"]
                    rsfinsert["Most_Recent_Action"]=reqjson["Most_recent_action"]
                    rsfinsert["Number_of_Black_Carts_Delivered"]=int(reqjson["Number_of_Black_Carts_Delivered"])
                    if reqjson["ssa"]:rsfinsert["SSA"]=int(reqjson["ssa"])

                elif reqjson["type_of_request"]=="Rodent Baiting/Rat Complaint":
                    rsfinsert["Current_Activity"]=reqjson["current_activity"]
                    rsfinsert["Most_Recent_Action"]=reqjson["Most_recent_action"]
                    if reqjson["Number_of_Premises_Baited"]:rsfinsert["Number_of_Premises_Baited"]=int(reqjson["Number_of_Premises_Baited"])
                    if reqjson["Number_of_Premises_with_Garbage"]:rsfinsert["Number_of_Premises_with_Garbage"]=int(reqjson["Number_of_Premises_with_Garbage"])
                    if reqjson["Number_of_Premises_ with_Rats"]:rsfinsert["Number_of_Premises_ with_Rats"]=int(reqjson["Number_of_Premises_ with_Rats"])
    
                elif reqjson["type_of_request"]=="Tree Trim":
                    rsfinsert["Location_of_Trees"]=reqjson["Location_of_Trees"]
                elif reqjson["type_of_request"]=="Abandoned Vehicle Complaint":
                    rsfinsert["Current_Activity"]=reqjson["current_activity"]
                    rsfinsert["Most_Recent_Action"]=reqjson["Most_recent_action"]
                    rsfinsert["License_Plate"]=reqjson["License_Plate"]
                    rsfinsert["Vehicle_Make/Model"]=reqjson["Vehicle_Make/Model"]
                    rsfinsert["Vehicle_Color"]=reqjson["Vehicle_Color"]
                    if reqjson["How_Many_Days_Has_the_Vehicle_Been_Reported_as_Parked?"]:rsfinsert["How_Many_Days_Has_the_Vehicle_Been_Reported_as_Parked?"]=int(reqjson["How_Many_Days_Has_the_Vehicle_Been_Reported_as_Parked?"])


       #     elif reqjson["type_of_request"]=="Alley Light Out":

                elif reqjson["type_of_request"]=="Sanitation Code Violation":
                    rsfinsert["What_is_the_Nature_of_this_Code_Violation?"]=reqjson["What_is_the_Nature_of_this_Code_Violation?"]

                elif reqjson["type_of_request"]=="Tree Debris":
                    rsfinsert["Current_Activity"]=reqjson["current_activity"]
                    rsfinsert["Most_Recent_Action"]=reqjson["Most_recent_action"]
                    rsfinsert["Where_is_the_debris_located?"]=reqjson["Where_is_the_debris_located?"]

          #  elif reqjson["type_of_request"]=="Street Lights - All/Out":

                res = db.data.insert_one(rsfinsert)
                print('Created request:',res.inserted_id)
                return dumps({"Created request":res.inserted_id}),200,{'Content-Type': 'application/json; charset=utf-8'}
    except:
        return dumps({'Error':'Please enter correct data!'}),400,{'Content-Type': 'application/json; charset=utf-8'}



@app.route('/avg_completion_time_per_service',methods=['GET'])
def query5():
    try:
        reqjson=request.get_json()
        print(reqjson)
        start_date=dateutil.parser.parse(reqjson['start_date'])
        print(start_date)
        end_date=dateutil.parser.parse(reqjson['end_date'])
        print(end_date)
        pipeline=[
                   	{"$match": { "$and": [{"Completion_Date":{"$ne":None}},{"Creation_Date":{"$gte":start_date, "$lte": end_date}}]}},
                        {"$group": {"_id": "$Type_of_Service_Request","avg_seconds": { "$avg": {"$divide": [{"$subtract":["$Completion_Date","$Creation_Date"]}, 1000]}}}}
                 ]
        data = db.data.aggregate(pipeline)

        return dumps({"Avg completion time for Time range:  "+reqjson['start_date']+" to "+reqjson['end_date']:data}),200,{'Content-Type': 'application/json; charset=utf-8'}
    except:
        res={'Error':'You probably entered wrong data!',"Help":"Enter any date in any format like YYYY-MM-DD HH:MM:SS"}
        return jsonify(res),400,{'Content-Type': 'application/json; charset=utf-8'}    

@app.route('/most_common_request_in_bounding_box',methods=['GET'])
def query6():
    try:
        reqjson=request.get_json()
        print(reqjson)
        dt=dateutil.parser.parse(reqjson['date'])
        minLon=float(reqjson['minLon'])
        maxLon=float(reqjson['maxLon'])
        minLat=float(reqjson['minLat'])
        maxLat=float(reqjson['maxLat'])
        pipeline=[
                { "$match": { "$and": [{"Creation_Date":{"$eq":dt}},{"Location_Point":{ "$geoWithin":{"$polygon":[[minLon,minLat],[maxLon,minLat],[maxLon,maxLat],[minLon,maxLat],[minLon,minLat]]}}}]}},
                {"$group": {"_id": "$Type_of_Service_Request","count": { "$sum": 1 }}},
	        {"$sort":{"count":-1}},
        	{"$limit":1}
            ]

        data = db.data.aggregate(pipeline)
        return dumps({"Most common type request in box for Time :  "+reqjson['date']:data}),200,{'Content-Type': 'application/json; charset=utf-8'}


    except:
        res={'Error':'You probably entered wrong data!',"Help":"Enter any date in any format like YYYY-MM-DD HH:MM:SS, minLat,maxLat,minLon,maxLon"}
        return jsonify(res),400,{'Content-Type': 'application/json; charset=utf-8'}


@app.route('/50_most_upvoted_requests',methods=['GET'])
def query7():
    try:
        reqjson=request.get_json()
        print(reqjson)
        dt=dateutil.parser.parse(reqjson['date'])

        pipeline=[
                    { "$match": {"Creation_Date":{"$eq":dt}}},
                    {"$project":{"total":{"$size":"$upvotes"}}},
                    {"$sort":{"total":-1}},
                    {"$limit":50}

                ]
        data = db.data.aggregate(pipeline)
        return dumps({"50 most upvoted requests for date : "+reqjson['date']:data}),200,{'Content-Type': 'application/json; charset=utf-8'}
    except:
        res={'Error':'You probably entered wrong data!',"Help":"Enter any date in any format like YYYY-MM-DD HH:MM:SS"}
        return jsonify(res),400,{'Content-Type': 'application/json; charset=utf-8'}

@app.route('/50_most_active_citizens',methods=['GET'])
def query8():
    pipeline=[
                {"$project":{"Name":"$name","Surname":"$surname","total":{"$size":"$votes"}}},
                {"$sort":{"total":-1}},
                {"$limit":50}
            ]
    data = db.citizen.aggregate(pipeline)
    return dumps({"50 most active citizens":data}),200,{'Content-Type': 'application/json; charset=utf-8'}


@app.route('/query11',methods=['GET'])
def query11():
    reqjson=request.get_json()
    name=reqjson['name']
    surname=reqjson['surname']
    doc = db.citizen.find_one({"name":name,"surname":surname})
    pipeline=[
                {"$unwind":"$votes"},
                {"$match":{"votes":doc["_id"]}},
                {"$project":{"Ward":"$Ward"}}
            ]
    data = db.data.aggregate(pipeline)
    return dumps({"Wards":data}),200,{'Content-Type': 'application/json; charset=utf-8'}


@app.route('/query4',methods=["GET"])
def query4():
    reqjson=request.get_json()
    type_of_request=reqjson['type_of_request']
    if type_of_request in all_types_of_request:
        pipeline=[
                    {"$match": { "$and":[{"Ward":{"$ne":""}},{"Type_of_Service_Request": type_of_request}]}},
                    {"$group": { "_id": "$Ward", "count": {"$sum": 1}}},
                    {"$sort": {"count": 1}},
                    {"$limit": 3}
                ]
            
        data=db.data.aggregate(pipeline)
        return dumps({"Result":data}),200,{'Content-Type': 'application/json; charset=utf-8'}
    else:
        res={'Error':'You entered an incorrect type of request!',"Help":"Please enter one of the following posible types.","Types of Request":all_types_of_request}
        return jsonify(res),400

@app.route('/query9',methods=['GET'])
def query9():
    pipeline=[
                {"$unwind":"$upvotes"},
                {"$group": {"_id": {"voter":"upvotes","ward":"$Ward"},"cnt": {"$sum":1}}},
                {"$group": {"_id":"$_id.voter", "cnt":{"$sum":1}}},
                {"$limit":50}  
            ]
    data = db.data.aggregate(pipeline)
    return dumps({"result":data}),200,{'Content-Type': 'application/json; charset=utf-8'}

if __name__ == '__main__':
    app.run(debug=True)
