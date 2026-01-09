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
r = requests.post(apiUrl, data=data)
print('HTTP Status: ' + str(r.status_code))
metaDataDf = pd.DataFrame(r.json())

fullBranchingLogics = []
for index, row in metaDataDf.iterrows():
    if row['branching_logic'] != '':
        fullBranchingLogics.append([row['branching_logic'], row['field_name']])

processes = []
for branchingLogic in fullBranchingLogics:
    process = []
    logic = branchingLogic[0]
    while logic !=  '':
        idx1 = logic.index('[') + 1
        idx2 = logic.index(']')
        verVar = logic[idx1:idx2]

        idx1 = logic.index("'") + 1
        idx2 = idx1 + 1
        verNum = logic[idx1:idx2]

        count = idx2 + 1

        if count != len(logic):
            logic = logic[count:]

            if logic[0] == ' ':
                logic = logic[1:]

            if logic[0] == 'o':
                process.append([[verVar, branchingLogic[1]], verNum])
                process.append('or')
            else:
                process.append([[verVar, branchingLogic[1]], verNum])
                process.append('and')

            count = logic.index('[')
            logic = logic[count:]
        else:
            process.append([[verVar, branchingLogic[1]], verNum])
            logic = ''

    processes.append(process)

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
redcapDB = pd.DataFrame(r.json())

count = 0
auxBool = False
wrongRows = []
redcapDB = redcapDB.fillna('')
for index, row in redcapDB.iterrows():
    for process in processes:
        count += 1
        processSize = len(process)
        if processSize == 2:
            try:
                #print(row[process[0][1]])
                if row[process[0][1]] != '':
                    if row[process[0][0]] == process[1]:
                        auxBool = True
                    else:
                        wrongRows.append(process)

            except KeyError:
                print(f"{process[0][1]} not in dataframe")
        else:
            try:
                conditionString = ""
                if row[process[0][0][1]] != '':
                    for item in process:
                        if item in ['and', 'or']:
                            conditionString += item
                            conditionString += " "
                        else:
                            if row[item[0][0]] == item[1]:
                                conditionString += "1 "
                            else:
                                conditionString += "0 "
                    if eval(conditionString) == 0:
                        wrongRows.append([index, process[0][0][0]])
                        count -= 1

            except KeyError:
                print(f"{process[0][0][1]} not in dataframe")
        
        auxBool = False

if wrongRows == []:
    print("There are no wrong rows! Success!")
else:
    print(f"There are {len(wrongRows)}, not a success :(\nWrong Rows:")
    for row in wrongRows:
        print(row)

print(f'{(count / (redcapDB.shape[0] * len(processes))) * 100}% Quality')