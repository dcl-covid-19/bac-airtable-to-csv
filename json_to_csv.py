import argparse
import csv
import json
import copy


def read_from_file(filename):
    with open(filename) as f:
        return json.load(f)


def get_schema():
    with open('schema.json') as f:
        return json.load(f)


def get_value(input_records, table_name, id, path, include_all=False, int_cast=False):
    try:
        if path[0] == '*':
            return get_value(input_records, table_name, id, path[1:], include_all=True, int_cast=int_cast)
        elif path[0] == '#':
            return get_value(input_records, table_name, id, path[1:], include_all, int_cast=True)
        else:
            if '.' in path:
                base, rest = path.split('.', 1)
                parts = base.split('/')
                fieldname, tablename = parts if len(parts) > 1 else (base, base)
                ids = input_records[table_name][id]["fields"][fieldname]
                if include_all:
                    return ','.join([get_value(input_records, tablename, child, rest, include_all, int_cast) for child in ids])
                else:
                    return get_value(input_records, tablename, ids[0], rest, include_all, int_cast)
            else:
                if int_cast:
                    try:
                        return int(input_records[table_name][id]["fields"][path])
                    except ValueError:
                        try:
                            return float(input_records[table_name][id]["fields"][path])
                        except ValueError:
                            return ''
                else:
                    rtn = input_records[table_name][id]["fields"][path]
                    if include_all and isinstance(rtn, list):
                        return ','.join(rtn)
                    else:
                        return rtn
    except KeyError:
        return ''


def transform_records(input_records, schema):
    transformed_records = []
    i = 0
    for service_id in input_records['services']:
        transformed_record = {}
        for k, v in schema.items():
            transformed_record[k] = get_value(input_records, 'services', service_id, v)
        transformed_records.append(transformed_record)
    return transformed_records


def hardcoded_record(transformed_record):
    hardcoded_record = copy.deepcopy(transformed_record)

    # med
    hardcoded_record["med_testing"] = "COVID Testing" in transformed_record["med_area"]
    hardcoded_record["med_primary_care"] = "Primary care" in transformed_record["med_area"]
    hardcoded_record["med_pediatrics"] = "Pediatrics" in transformed_record["med_area"]
    hardcoded_record["med_senior"] = "Senior care" in transformed_record["med_area"]
    hardcoded_record["med_women"] = "Women's health care" in transformed_record["med_area"]
    hardcoded_record["med_urgent_care"] = "Urgent care" in transformed_record["med_area"]
    hardcoded_record["med_dental"] = "Dental care" in transformed_record["med_area"]
    hardcoded_record["med_vision"] = "Vision care" in transformed_record["med_area"]
    hardcoded_record["med_pharmacy"] = "Pharmacy" in transformed_record["med_area"]
    hardcoded_record["med_mental_health"] = "Mental health care" in transformed_record["med_area"]
    hardcoded_record["med_hotline"] = "Hotline" in transformed_record["med_area"]
    hardcoded_record["med_addiction"] = "Addiction and recovery support" in transformed_record["med_area"]
    hardcoded_record["med_domestic_violence"] = "Victim of domestic violence care" in transformed_record["med_area"]

    # legal
    hardcoded_record["legal_housing"] = "Housing support" in transformed_record["legal_area"]
    hardcoded_record["legal_worker_protection"] = "Workers' rights support" in transformed_record["legal_area"]
    hardcoded_record["legal_healthcare"] = "Healthcare benefits support" in transformed_record["legal_area"]
    hardcoded_record["legal_immigration"] = "Immigration support" in transformed_record["legal_area"]
    hardcoded_record["legal_criminal"] = "Criminal justice support" in transformed_record["legal_area"]
    hardcoded_record["legal_domviolence"] = "Domestic violence support" in transformed_record["legal_area"]
    hardcoded_record["legal_contracts"] = "Contract law support" in transformed_record["legal_area"]
    hardcoded_record["legal_protester_defense"] = "Protester defense" in transformed_record["legal_area"]

    # financial
    hardcoded_record["fin_grocery"] = "Cash for groceries" in transformed_record["cash_area"]
    hardcoded_record["fin_housing"] = "Cash for rent" in transformed_record["cash_area"]
    hardcoded_record["fin_legal"] = "Cash for legal fees" in transformed_record["cash_area"]
    hardcoded_record["fin_medical"] = "Cash for medical fees" in transformed_record["cash_area"]
    hardcoded_record["fin_utilities"] = "Cash for utilities" in transformed_record["cash_area"]

    # domestic violence
    hardcoded_record["dv_medical"] = "DV medical support" in transformed_record["dv_area"]
    hardcoded_record["dv_mental_health"] = "DV mental health support" in transformed_record["dv_area"]
    hardcoded_record["dv_housing"] = "DV housing and shelter" in transformed_record["dv_area"]
    hardcoded_record["dv_legal"] = "DV legal support" in transformed_record["dv_area"]
    hardcoded_record["dv_crisis"] = "DV crisis support" in transformed_record["dv_area"]
    hardcoded_record["dv_referrals"] = "DV referrals" in transformed_record["dv_area"]


    # snap/wic
    hardcoded_record["SNAP"] = "Accepts SNAP" in transformed_record["foodstamp_area"]
    hardcoded_record["WIC"] = "Accepts WIC" in transformed_record["foodstamp_area"]

    # details
    hardcoded_record["public"] = "Open to public" in transformed_record["details"]
    hardcoded_record["children"] = "Children" in transformed_record["details"]
    hardcoded_record["homeless"] = "Homeless" in transformed_record["details"]
    hardcoded_record["uninsured"] = "Uninsured" in transformed_record["details"]
    hardcoded_record["residents"] = "Residents only" in transformed_record["details"]
    hardcoded_record["immigrants"] = "Accepts WIC" in transformed_record["details"]
    hardcoded_record["low_income"] = "Low-income" in transformed_record["details"]
    hardcoded_record["women"] = "Women" in transformed_record["details"]
    hardcoded_record["disabled"] = "Disabled" in transformed_record["details"]
    hardcoded_record["native_american"] = "Native American" in transformed_record["details"]
    hardcoded_record["seniors"] = "Seniors" in transformed_record["details"]
    hardcoded_record["accepts_medical"] = "Accepts MediCal" in transformed_record["details"]
    hardcoded_record["discount_medical"] = "Accepts MediCal" in transformed_record["details"]
    hardcoded_record["must_show_id"] = "Must show ID" in transformed_record["details"]
    hardcoded_record["all_children"] = "All Children" in transformed_record["details"]
    hardcoded_record["enrolled_children"] = "Only Students Enrolled in District" in transformed_record["details"]

    # else
    hardcoded_record["free"] = "Free" in transformed_record["payment_options"]
    hardcoded_record["sliding_scale"] = "Sliding scale" in transformed_record["payment_options"]
    hardcoded_record["financial_assistance"] = "Financial assistance" in transformed_record["payment_options"]
    hardcoded_record["require_child"] = "Child need be present" in transformed_record["freefood_area"]
    hardcoded_record["contact"] = transformed_record["number"]
    hardcoded_record["status"] = transformed_record["status"] == "Open"
    hardcoded_record["temp_closed"] = transformed_record["status"] == "Temporarily Closed"
    hardcoded_record["delivery"] = "Delivery" in transformed_record["service_options"]
    hardcoded_record["curbside_pickup"] = "Curbside Pickup" in transformed_record["service_options"]
    hardcoded_record["drive_thru"] = "Drive Thru" in transformed_record["service_options"]
    hardcoded_record["in_person"] = "In person" in transformed_record["service_options"]
    hardcoded_record["telehealth"] = "Telehealth" in transformed_record["service_options"]
    hardcoded_record["telelegal"] = "Telelegal" in transformed_record["service_options"]
    hardcoded_record["ballot_outdoor"] = "Outdoor Ballot Drop-Off" in transformed_record["service_options"]
    hardcoded_record["ballot_indoor"] = "Indoor Ballot Drop-Off" in transformed_record["service_options"]
    hardcoded_record["call_in_advance"] = "Call in advance" in transformed_record["application_process"]
    hardcoded_record["must_show_id"] = "Must show ID" in transformed_record["application_process"]
    hardcoded_record["alameda"] = "Alameda" in transformed_record["service_area"]
    hardcoded_record["san_francisco"] = "San Francisco" in transformed_record["service_area"]
    hardcoded_record["contra_costa"] = "Contra Costa" in transformed_record["service_area"]
    hardcoded_record["santa_clara"] = "Santa Clara" in transformed_record["service_area"]
    hardcoded_record["san_mateo"] = "San Mateo" in transformed_record["service_area"]
    hardcoded_record["solano"] = "Solano" in transformed_record["service_area"]
    hardcoded_record["sonoma"] = "Sonoma" in transformed_record["service_area"]
    hardcoded_record["napa"] = "Napa" in transformed_record["service_area"]
    hardcoded_record["santa_cruz"] = "Santa Cruz" in transformed_record["service_area"]
    hardcoded_record["san_benito"] = "San Benito" in transformed_record["service_area"]
    hardcoded_record["sacramento"] = "Sacramento" in transformed_record["service_area"]
    hardcoded_record["farmers_market"] = transformed_record["taxonomy"] == "Farmer's Market"
    hardcoded_record["meal_student"] = transformed_record["taxonomy"] == "School Meals"
    hardcoded_record["free_groceries"] = transformed_record["taxonomy"] == "Groceries" and "Free" in transformed_record["payment_options"]
    hardcoded_record["bob"] = transformed_record["taxonomy"] == "Black-owned Restaurant"
    if transformed_record["opens_at"] and transformed_record["closes_at"]:
        hardcoded_record["mon"] = "{} - {}".format(transformed_record["opens_at"], transformed_record["closes_at"]) if "Monday" in transformed_record["weekday"] else ""
        hardcoded_record["tues"] = "{} - {}".format(transformed_record["opens_at"], transformed_record["closes_at"]) if "Tuesday" in transformed_record["weekday"] else ""
        hardcoded_record["wed"] = "{} - {}".format(transformed_record["opens_at"], transformed_record["closes_at"]) if "Wednesday" in transformed_record["weekday"] else ""
        hardcoded_record["thr"] = "{} - {}".format(transformed_record["opens_at"], transformed_record["closes_at"]) if "Thursday" in transformed_record["weekday"] else ""
        hardcoded_record["fri"] = "{} - {}".format(transformed_record["opens_at"], transformed_record["closes_at"]) if "Friday" in transformed_record["weekday"] else ""
        hardcoded_record["sat"] = "{} - {}".format(transformed_record["opens_at"], transformed_record["closes_at"]) if "Saturday" in transformed_record["weekday"] else ""
        hardcoded_record["sun"] = "{} - {}".format(transformed_record["opens_at"], transformed_record["closes_at"]) if "Sunday" in transformed_record["weekday"] else ""
    if transformed_record["add_hours"]:
        if transformed_record["add_day"] == "Monday":
            hardcoded_record["mon"] += (", " if hardcoded_record["mon"] else '') + transformed_record["add_hours"]
        if transformed_record["add_day"] == "Tuesday":
            hardcoded_record["tues"] += (", " if hardcoded_record["tues"] else '') + transformed_record["add_hours"]
        if transformed_record["add_day"] == "Wednesday":
            hardcoded_record["wed"] += (", " if hardcoded_record["wed"] else '') + transformed_record["add_hours"]
        if transformed_record["add_day"] == "Thursday":
            hardcoded_record["thr"] += (", " if hardcoded_record["thr"] else '') + transformed_record["add_hours"]
        if transformed_record["add_day"] == "Friday":
            hardcoded_record["fri"] += (", " if hardcoded_record["fri"] else '') + transformed_record["add_hours"]
        if transformed_record["add_day"] == "Saturday":
            hardcoded_record["sat"] += (", " if hardcoded_record["sat"] else '') + transformed_record["add_hours"]
        if transformed_record["add_day"] == "Sunday":
            hardcoded_record["sun"] += (", " if hardcoded_record["sun"] else '') + transformed_record["add_hours"]
    RESOURCE_DICT = {
        "Black-owned Restaurant": "restaurant",
        "Health": "health",
        "Legal": "legal",
        "Financial Assistance": "cash_assistance",
        "Groceries": "grocery",
        "Prepared Meals": "meal",
        "Farmer's Market": "grocery",
        "School Meals": "meal",
        "Domestic Violence Resource": "dv_resources",
        "Ballot Drop Off": "ballot_drop"
    }
    hardcoded_record["resource"] = RESOURCE_DICT.get(transformed_record["taxonomy"], '')
    if transformed_record["taxonomy"] == "Health" and "Mental health care" in transformed_record["med_area"]:
        hardcoded_record["resource"] = "mental_health"

    # remove
    del hardcoded_record["taxonomy"]
    del hardcoded_record["taxonomy_parent"]
    del hardcoded_record["number"]
    del hardcoded_record["extension"]
    del hardcoded_record["details"]
    del hardcoded_record["service_options"]
    del hardcoded_record["payment_options"]
    del hardcoded_record["med_area"]
    del hardcoded_record["legal_area"]
    del hardcoded_record["cash_area"]
    del hardcoded_record["dv_area"]
    del hardcoded_record["foodstamp_area"]
    del hardcoded_record["service_area"]
    del hardcoded_record["application_process"]
    del hardcoded_record["weekday"]
    del hardcoded_record["opens_at"]
    del hardcoded_record["closes_at"]
    del hardcoded_record["add_day"]
    del hardcoded_record["add_hours"]

    def to_int(x):
        return int(x == True) if type(x) == bool else x

    return { k: to_int(v) for (k, v) in hardcoded_record.items() }


def write_csv(transformed_records, schema, filename):
    with open(filename, mode='w') as f:
        writer = csv.DictWriter(f, fieldnames=list(transformed_records[0].keys()), quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for record in transformed_records:
            writer.writerow(record)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Convert JSON data from airtable_to_json.py to CSV')
    parser.add_argument('input_filename', help='Filename to read JSON from.')
    parser.add_argument('output_filename', help='Filename to write the output CSV to.')
    args = parser.parse_args()
    input_records = read_from_file(args.input_filename)
    schema = get_schema()
    transformed_records = transform_records(input_records, schema)
    transformed_records = list(map(hardcoded_record, transformed_records))
    write_csv(transformed_records, schema, args.output_filename)
