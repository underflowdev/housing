import requests
import time


fred_base = 'https://api.stlouisfed.org/fred/release/tables'
api_key = "put your api key here"
file_type = 'json'
include_observation_values = 'true'

release_id = '175'
element_id = '266159'
observation_date = '2001-01-01'

# foreach element_id (which is the state)
#    foreach year from 2000 to 2021
#       delay
#       request

payload = {
    'api_key': api_key,
    'file_type': file_type,
    'include_observation_values': include_observation_values,
    'release_id': release_id,
    'element_id': element_id,
    'observation_date': observation_date
}
states = [
    {'key': '266091', 'name': 'Alabama'},
    {'key': '266159', 'name': 'Alaska'},
    {'key': '266213', 'name': 'Arizona'},
    {'key': '266229', 'name': 'Arkansas'},
    {'key': '266305', 'name': 'California'},
    {'key': '266364', 'name': 'Colorado'},
    {'key': '266429', 'name': 'Connecticut'},
    {'key': '266438', 'name': 'Delaware'},
    {'key': '266442', 'name': 'Delaware'},
    {'key': '266444', 'name': 'Florida'},
    {'key': '266512', 'name': 'Georgia'},
    {'key': '266672', 'name': 'Hawaii'},
    {'key': '266677', 'name': 'Idaho'},
    {'key': '266722', 'name': 'Illinois'},
    {'key': '266825', 'name': 'Indiana'},
    {'key': '266918', 'name': 'Iowa'},
    {'key': '267018', 'name': 'Kansas'},
    {'key': '267124', 'name': 'Kentucky'},
    {'key': '267245', 'name': 'Louisiana'},
    {'key': '267310', 'name': 'Maine'},
    {'key': '267327', 'name': 'Maryland'},
    {'key': '267352', 'name': 'Massachusetts'},
    {'key': '267367', 'name': 'Michigan'},
    {'key': '267451', 'name': 'Minnesota'},
    {'key': '267539', 'name': 'Mississippi'},
    {'key': '267622', 'name': 'Missouri'},
    {'key': '267738', 'name': 'Montana'},
    {'key': '267795', 'name': 'Nebraska'},
    {'key': '267889', 'name': 'Nevada'},
    {'key': '267907', 'name': 'New Hampshire'},
    {'key': '267918', 'name': 'New Jersey'},
    {'key': '267940', 'name': 'New Mexico'},
    {'key': '267974', 'name': 'New York'},
    {'key': '268037', 'name': 'North Carolina'},
    {'key': '268138', 'name': 'North Dakota'},
    {'key': '268192', 'name': 'Ohio'},
    {'key': '268281', 'name': 'Oklahoma'},
    {'key': '268359', 'name': 'Oregon'},
    {'key': '268396', 'name': 'Pennsylvania'},
    {'key': '268464', 'name': 'Rhode Island'},
    {'key': '268470', 'name': 'South Carolina'},
    {'key': '268517', 'name': 'South Dakota'},
    {'key': '268584', 'name': 'Tennessee'},
    {'key': '268680', 'name': 'Texas'},
    {'key': '268935', 'name': 'Utah'},
    {'key': '268965', 'name': 'Vermont'},
    {'key': '268980', 'name': 'Virginia'},
    {'key': '269081', 'name': 'Washington'},
    {'key': '269121', 'name': 'West Virginia'},
    {'key': '269177', 'name': 'Wisconsin'},
    {'key': '269251', 'name': 'Wyoming'}
]

start_date = 2001
end_date = 2025
dates = []
while start_date < end_date:
    dates.append(f'{str(start_date)}-01-01')
    start_date += 1


for date in dates:
    for state in states:
        time.sleep(1)
        payload['observation_date'] = date
        payload['element_id'] = state.get('key')
        r = requests.get(fred_base, params=payload)
        path = f'./data/fred/{state.get("name")}-{date}.json'
        print(path)
        with open(path, "w") as outfile:
            outfile.write(r.text)
