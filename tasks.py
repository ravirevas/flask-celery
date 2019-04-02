from flask import Flask,session
import subprocess
from subprocess import check_output
mylist=[]

app = Flask(__name__)

app.secret_key = 'super secret key'

command="nohup bash /home/rvi/sample.sh "
command1="bash /home/rvi/sample.sh"

@app.route('/status/<rulesetname>')
def process(rulesetname):

    global mylist
    global poll
    a=command+" "+rulesetname+"&"
    print(a)



    if rulesetname not in mylist:
      #result_status=subprocess.call(a,shell=True)
      result_status = subprocess.Popen(a, shell=True)
      mylist.append(rulesetname)
      print("hey my list is")
      print(mylist)
      session['mylist']=mylist

      return rulesetname+"\n started"

    else:
        return "alredy running same task"+rulesetname


@app.route('/getstatus/<rulesetname>')
def process_get(rulesetname):

    a=session.get('mylist')
    session['mylist']=mylist.remove('ravi')
    b="ps -ef | grep sample.sh | grep bash | grep ravi"
    result_status = subprocess.Popen(b,stdout=subprocess.PIPE,shell=True)
    result=result_status.communicate()
    print(result)
    print("your list is")
    print(a)
    return "its running bro"+str(result)




if __name__=='__main__':
    app.run(debug=True)


