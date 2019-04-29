from metastore_model import *
from app4 import *


def generate_request_id(rulesetname):
    rule_set_name_id=rulesetname+"_"+str(datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"))
    return rule_set_name_id


def create_command_to_run(rulesetname,data_date,batch_date,sequence_number):
    full_command = "C:\\Users\\Ravi\\Desktop\\sample.py --rulesetname "+rulesetname+"  --data_date "+str(data_date) + " --batch_date "+str(batch_date) + " --seqnum "+str(sequence_number)
    return full_command


def check_for_dq_process(rulesetname,data_date,batch_date,sequence_number):
    b = "ps -ef | grep sample.sh | grep bash | grep -w " + "[" + rulesetname[0] + "]" + rulesetname[1:]  # fix this
    b="pgrep -f 'sample.sh --rulesetname DQ_check --datadate 1223344'"
    #result_status = subprocess.Popen(b, stdout=subprocess.PIPE, shell=True)
    #result = result_status.communicate()
    result="bash"
    if "bash" in str(result):
        result="Yes"
        return result
    else:
        result = "No"
        return result


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
def fetch_id_from_rule_log_id(rulesetname,data_date,batch_date):
    db.session.commit()
    if data_date is not None and batch_date is not None:
        print("#####Have all parameters#######")
        req_id=db.session.query(RuleLog.id,RuleLog.create_ts).filter(RuleLog.id.like(rulesetname+'%'),RuleLog.data_dt == data_date,RuleLog.batch_dt == batch_date).order_by(RuleLog.create_ts.desc()).first()
        id = req_id[0]
        return id
    elif data_date is not None and batch_date is  None :
        print("###No batch date#######")
        req_id = db.session.query(RuleLog.id, RuleLog.create_ts).filter(RuleLog.id.like(rulesetname + '%'),
                                                                        RuleLog.data_dt == data_date,
                                                                        ).order_by(
            RuleLog.create_ts.desc()).first()
        id = req_id[0]
        return id

    elif data_date is  None and batch_date is not None:
        print("##No datadate ##")
        req_id = db.session.query(RuleLog.id, RuleLog.create_ts).filter(RuleLog.id.like(rulesetname + '%'),
                                                                        RuleLog.batch_dt == batch_date,
                                                                        ).order_by(
            RuleLog.create_ts.desc()).first()
        id = req_id[0]
        return id

    else:
        print("#######just have rulesetname##########")
        req_id = db.session.query(RuleLog.id, RuleLog.create_ts).filter(RuleLog.id.like(rulesetname + '%'),
                                                                        ).order_by(
            RuleLog.create_ts.desc()).first()
        id = req_id[0]
        return id


def check_prev_datadate_is_done(rulesetname,datadate):
    req_id = db.session.query(RuleLog.id, RuleLog.create_ts).filter(RuleLog.id.like('%' + rulesetname + '%'),
                                                                    RuleLog.data_dt == datadate).order_by(
        RuleLog.create_ts.desc()).all()

    return req_id


