from metastore_model import *
from app4 import db


def generate_request_id(rulesetname):
    rule_set_name_id=rulesetname+"_"+str(datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"))
    return rule_set_name_id


def create_command_to_run(rulesetname,data_date,batch_date):
    command = "C:\\Users\\Ravi\\Desktop\\sample.py --rulesetnames "+rulesetname+"  --data_date "+str(data_date) + " --batch_date "+str(batch_date)
    #--rulesetname " + rulesetname + " --datadate " + str(data_date)
    #command1="bash /home/rvi/sample.sh --rulesetname " + rulesetname + " --datadate " + str(data_date) + " --batchdate"+batch_date+ " --startdate"+start_date+" --enddate"+end_date
    #print('##############')
    #print(command1)
    fullcommnad=command
    return fullcommnad


#check whether a rule exists or not
def is_ruleset_exists(rulesetname):
    result_status = db.session.query(RuleSet.name).filter_by(name=rulesetname).first()
    return result_status


def ruleset_start_time():
    ruleset_start_time = str(datetime.datetime.now())
    return ruleset_start_time



def ruleset_params_startime(rulesetname,date_time):
    ruleset_name_datetime = rulesetname + "_" + date_time
    return ruleset_name_datetime


def addtodict(mylist,user,rulesetname,date_time):
    rulesetname_date=rulesetname+"_"+date_time
    mylist.setdefault(user, []).append(rulesetname_date)
    return mylist

#add datadate here as function parameters
#check i like
def fetch_id_from_rule_log_id(rulesetname):
    db.session.commit()
    req_id=db.session.query(RuleLog.id,RuleLog.create_ts).filter(RuleLog.id.like(rulesetname+'%')).order_by(RuleLog.create_ts.desc()).first()
    if req_id is not None:
     id=req_id[0].replace("-","").replace(" ","").replace(":","")
     return id
    else:
     return "No such rule_id with datadate"




def check_prev_datadate_is_done(rulesetname,datadate):
    req_id = db.session.query(RuleLog.id, RuleLog.create_ts).filter(RuleLog.id.like('%' + rulesetname + '%'),
                                                                    RuleLog.data_dt == datadate).order_by(
        RuleLog.create_ts.desc()).all()

    return req_id
