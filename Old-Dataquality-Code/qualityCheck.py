import pandas as pd
import requests

token = 'CC585E1F8B3EC9212A45B258CFFD5E9E'
apiUrl = 'https://ncov.medsci.ox.ac.uk/api/'

data = {
    'token': token,
    'content': 'metadata',
    'format': 'json',
    'returnFormat': 'json'
}
r = requests.post(apiUrl,data=data)
print('HTTP Status: ' + str(r.status_code))
db1 = pd.DataFrame(r.json())

data = {
    'token': token,
    'content': 'record',
    'action': 'export',
    'format': 'json',
    'type': 'flat',
    'csvDelimiter': '',
    'rawOrLabel': 'raw',
    'rawOrLabelHeaders': 'raw',
    'exportCheckboxLabel': 'false',
    'exportSurveyFields': 'false',
    'exportDataAccessGroups': 'false',
    'returnFormat': 'json'
}
r = requests.post(apiUrl,data=data)
print('HTTP Status: ' + str(r.status_code))
db2 = pd.DataFrame(r.json())

#branching logic array
branchingLogic = []
for i in range(db1.shape[0]):
    if db1['branching_logic'][i] != '':
        field_name = db1['field_name'][i]

        idx1 = db1['branching_logic'][i].index('[') + 1
        idx2 = db1['branching_logic'][i].index(']')

        logic_field = db1['branching_logic'][i][idx1:idx2]

        idx3 = db1['branching_logic'][i].index("'") + 1
        idx4 = -1
        logic_number = db1['branching_logic'][i][idx3:idx4]

        branchingLogic.append([field_name, logic_field, logic_number])

count = 0
for i in range(db2.shape[0]):
    for logic in branchingLogic:
        if db2[logic[0]][i] == logic[2]:
            if db2[logic[1]] != '':
                count += 1
                
print('Data Quality:', ((db1.shape[0]-count) / db1.shape[0]) * 100,'%')