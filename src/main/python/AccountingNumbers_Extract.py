
################################ ACCOUNTING NUMBERS #############################################

# Import af regnskabsdata baseret på listen af CVR numre
import json
import requests
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import re
import csv
import pandas as pd
from datetime import datetime
import itertools as it


auth = ('MicTor', 'c0041564-de17-48b7-874a-90579cbd41f2')

def chunkylonky(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

########################## LIST OF CVR NO. TO BE LOOPED OVER ####################################
with open('C:/Projekter/ERST/Input_to_python_scripts/reprimand.csv') as f:
    next(f)
    CVR = [line.split(';')[0] for line in f]

################################ TYPE OF COMPANY #############################################
pattern_Consolidated = r"(?<=:)ConsolidatedSoloDimension$"
pattern_PL = r"(?<=:)ProfitLoss$"
pattern_Assets = r"(?<=:)Assets$"
pattern_NCAssets = r"(?<=:)NoncurrentAssets$"
pattern_IntAssets = r"(?<=:)IntangibleAssets$"
pattern_PPE = r"(?<=:)PropertyPlantAndEquipment$"
pattern_LB = r"(?<=:)LandAndBuildings$"
pattern_IP = r"(?<=:)InvestmentProperty$"
pattern_LIAR = r"(?<=:)LongtermInvestmentsAndReceivables$"
pattern_CA = r"(?<=:)CurrentAssets$"
pattern_Inventories = r"(?<=:)Inventories$"
pattern_SR = r"(?<=:ShorttermReceivables$)"
pattern_SI = r"(?<=:)ShorttermInvestments$"
pattern_CACE = r"(?<=:)CashAndCashEquivalents$"
pattern_Equity = r"(?<=:)Equity$"
pattern_LOTP = r"(?<=:)LiabilitiesOtherThanProvisions$"
pattern_LLOTP = r"(?<=:)LongtermLiabilitiesOtherThanProvisions$"
pattern_SLOTP = r"(?<=:)ShorttermLiabilitiesOtherThanProvisions$"
pattern_Provisions = r"(?<=:)Provisions$"
pattern_CDTA = r"(?<=:)CurrentDeferredTaxAssets$"
pattern_RR = r"(?<=:)RevaluationReserve$"
pattern_DTB = r"(?<=:)DebtToBanks$"
pattern_MD = r"(?<=:)MortgageDebt$"
pattern_PTGE = r"(?<=:)PayablesToGroupEnterprises$"
pattern_ODRB = r"(?<=:)OtherDebtRaisedByIssuanceOfBonds$"
pattern_POWIP = r"(?<=:)PrepaymentsOfWorkInProgress$"
pattern_CWIP = r"(?<=:)ContractWorkInProgress$"

Pull_PL = list()
Pull_Assets = list()
Pull_NCAssets = list()
Pull_IntAssets = list()
Pull_PPE = list()
Pull_LB = list()
Pull_IP = list()
Pull_LIAR = list()
Pull_CA = list()
Pull_Inventories = list()
Pull_SR = list()
Pull_SI = list()
Pull_CACE = list()
Pull_Equity = list()
Pull_LOTP = list()
Pull_LLOTP = list()
Pull_SLOTP = list()
Pull_Provisions = list()
Pull_NOSE = list()
Pull_CDTA = list()
Pull_RR = list()
Pull_DTB = list()
Pull_MD = list()
Pull_PTGE = list()
Pull_ODRB = list()
Pull_POWIP = list()
Pull_CWIP = list()

# This creates a loop through one specific CVR
for x in CVR:
    print(x)
    url = 'https://distribution.virk.dk:443'
    index = 'xbrl-prod'
    client = Elasticsearch(url, http_auth=auth, timeout=300, verify_certs=True)
    response = client.search(index=index,
                             body='{"query": {"bool": {"must": [{"term": {"Report.metadata.CVR": "' + str(x)
                                  + '"}}]}}}')

# This takes all the data for one specific CVR for all years
# This will loop through all annual reports
    AllFactChunks = json.dumps(response['hits']['hits'])
    Data = json.loads(AllFactChunks)

# Makes it possible to loop through all chunks in a fact of an annual report
    for y in range(0, len(Data)):
        Facts = list()
        Facts.extend(Data[y]["_source"]["Report"]["facts"])

# Loops through all facts to get the correct accounting entry
    # GETS PROFITLOSS
        for z in range(0, len(Facts)):
            # checks if ProfitLoss is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_PL, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_PL.extend(['ProfitLoss', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_PL, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_PL.extend(['ProfitLoss',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "ProfitLoss" not in Pull_PL:
            Pull_PL.extend(['ProfitLoss', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS ASSETS
        for z in range(0, len(Facts)):
            # checks if Assets is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_Assets, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_Assets.extend(['Assets', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_Assets, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_Assets.extend(['Assets',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "Assets" not in Pull_Assets:
            Pull_Assets.extend(['Assets', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS NONCURRENT ASSESTS
        for z in range(0, len(Facts)):
            # checks if NonCurrent Assets is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_NCAssets, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_NCAssets.extend(['NoncurrentAssets', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_NCAssets, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_NCAssets.extend(['NoncurrentAssets',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "NoncurrentAssets" not in Pull_NCAssets:
            Pull_NCAssets.extend(['NoncurrentAssets', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS INTANGIBLE ASSESTS
        for z in range(0, len(Facts)):
            # checks if Intangible Assets is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_IntAssets, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_IntAssets.extend(['IntangibleAssets', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_IntAssets, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_IntAssets.extend(['IntangibleAssets',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "IntangibleAssets" not in Pull_IntAssets:
            Pull_IntAssets.extend(['IntangibleAssets', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS PROPERTY PLANT AND EQUIPMENT
        for z in range(0, len(Facts)):
            # checks if Property Plant and Equipment is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_PPE, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_PPE.extend(['PropertyPlantAndEquipment', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_PPE, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_PPE.extend(['PropertyPlantAndEquipment',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "PropertyPlantAndEquipment" not in Pull_PPE:
            Pull_PPE.extend(['PropertyPlantAndEquipment', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS LAND AND BUILDINGS
        for z in range(0, len(Facts)):
            # checks if Land and Building is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_LB, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_LB.extend(['LandAndBuildings', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_LB, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_LB.extend(['LandAndBuildings',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "LandAndBuildings" not in Pull_LB:
            Pull_LB.extend(['LandAndBuildings', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS INVESTMENT PROPERTY
        for z in range(0, len(Facts)):
            # checks if Investment Property is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_IP, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_IP.extend(['InvestmentProperty', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_IP, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_IP.extend(['InvestmentProperty',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "InvestmentProperty" not in Pull_IP:
            Pull_IP.extend(['InvestmentProperty', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS LONGTERM INVESTMENTS AND RECEIVABLES
        for z in range(0, len(Facts)):
            # checks if Investment and receivables is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_LIAR, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_LIAR.extend(['LongtermInvestmentsAndReceivables', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_LIAR, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_LIAR.extend(['LongtermInvestmentsAndReceivables',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "LongtermInvestmentsAndReceivables" not in Pull_LIAR:
            Pull_LIAR.extend(['LongtermInvestmentsAndReceivables', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS CURRENT ASSETS
        for z in range(0, len(Facts)):
            # checks if Current assets is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_CA, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_CA.extend(['CurrentAssets', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_CA, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_CA.extend(['CurrentAssets',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "CurrentAssets" not in Pull_CA:
            Pull_CA.extend(['CurrentAssets', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS INVENTORIES
        for z in range(0, len(Facts)):
            # checks if Inventories is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_Inventories, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_Inventories.extend(['Inventories', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_Inventories, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_Inventories.extend(['Inventories',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "Inventories" not in Pull_Inventories:
            Pull_Inventories.extend(['Inventories', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS SHORTTERM RECEIVABLES
        for z in range(0, len(Facts)):
            # checks if shortterm receivables is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_SR, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_SR.extend(['ShorttermReceivables', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_SR, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_SR.extend(['ShorttermReceivables',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "ShorttermReceivables" not in Pull_SR:
            Pull_SR.extend(['ShorttermReceivables', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS SHORTTERM INVESTMENTS
        for z in range(0, len(Facts)):
            # checks if shortterm investments is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_SI, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_SI.extend(['ShorttermInvestments', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_SI, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_SI.extend(['ShorttermInvestments',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "ShorttermInvestments" not in Pull_SI:
            Pull_SI.extend(['ShorttermInvestments', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS CASH AND CASH EQUIVALENTS
        for z in range(0, len(Facts)):
            # checks if cash and cash equivalents is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_CACE, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_CACE.extend(['CashAndCashEquivalents', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_CACE, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_CACE.extend(['CashAndCashEquivalents',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "CashAndCashEquivalents" not in Pull_CACE:
            Pull_CACE.extend(['CashAndCashEquivalents', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS EQUITY
        for z in range(0, len(Facts)):
            # checks if equity is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_Equity, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_Equity.extend(['Equity', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_Equity, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_Equity.extend(['Equity',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "Equity" not in Pull_Equity:
            Pull_Equity.extend(['Equity', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS LIABILITIES OTHER THAN PROVISIONS
        for z in range(0, len(Facts)):
            # checks if LOTP is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_LOTP, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_LOTP.extend(['LiabilitiesOtherThanProvisions', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_LOTP, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_LOTP.extend(['LiabilitiesOtherThanProvisions',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "LiabilitiesOtherThanProvisions" not in Pull_LOTP:
            Pull_LOTP.extend(['LiabilitiesOtherThanProvisions', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS LONGTERM LIABILITIES OTHER THAN PROVISIONS
        for z in range(0, len(Facts)):
            # checks if LLOTP is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_LLOTP, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_LLOTP.extend(['LongtermLiabilitiesOtherThanProvisions', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_LLOTP, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_LLOTP.extend(['LongtermLiabilitiesOtherThanProvisions',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "LongtermLiabilitiesOtherThanProvisions" not in Pull_LLOTP:
            Pull_LLOTP.extend(['LongtermLiabilitiesOtherThanProvisions', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS SHORTTERM LIABILITIES OTHER THAN PROVISIONS
        for z in range(0, len(Facts)):
            # checks if SLOTP is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_SLOTP, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_SLOTP.extend(['ShorttermLiabilitiesOtherThanProvisions', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_SLOTP, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_SLOTP.extend(['ShorttermLiabilitiesOtherThanProvisions',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "ShorttermLiabilitiesOtherThanProvisions" not in Pull_SLOTP:
            Pull_SLOTP.extend(['ShorttermLiabilitiesOtherThanProvisions', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS PROVISIONS
        for z in range(0, len(Facts)):
            # checks if Provisions is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_Provisions, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_Provisions.extend(['Provisions', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_Provisions, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_Provisions.extend(['Provisions',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "Provisions" not in Pull_Provisions:
            Pull_Provisions.extend(['Provisions', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS CURRENT DEFERRED TAX ASSETS
        for z in range(0, len(Facts)):
            # checks if DTA is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_CDTA, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_CDTA.extend(['CurrentDeferredTaxAssets', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_CDTA, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_CDTA.extend(['CurrentDeferredTaxAssets',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "CurrentDeferredTaxAssets" not in Pull_CDTA:
            Pull_CDTA.extend(['CurrentDeferredTaxAssets', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS REVALUATION RESERVE
        for z in range(0, len(Facts)):
            # checks if Revaluation Reserve is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_RR, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_RR.extend(['RevaluationReserve', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_RR, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_RR.extend(['RevaluationReserve',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "RevaluationReserve" not in Pull_RR:
            Pull_RR.extend(['RevaluationReserve', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS DEBT TO BANKS
        for z in range(0, len(Facts)):
            # checks if Debt to Banks is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_DTB, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_DTB.extend(['DebtToBanks', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_DTB, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_DTB.extend(['DebtToBanks',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "DebtToBanks" not in Pull_DTB:
            Pull_DTB.extend(['DebtToBanks', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS MORTGAGE DEBT
        for z in range(0, len(Facts)):
            # checks if Mortgage Debt is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_MD, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_MD.extend(['MortgageDebt', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_MD, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_MD.extend(['MortgageDebt',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "MortgageDebt" not in Pull_MD:
            Pull_MD.extend(['MortgageDebt', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS PAYABLES TO GROUP ENTERPRISES
        for z in range(0, len(Facts)):
            # checks if PTGE is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_PTGE, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_PTGE.extend(['PayablesToGroupEnterprises', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_PTGE, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_PTGE.extend(['PayablesToGroupEnterprises',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "PayablesToGroupEnterprises" not in Pull_PTGE:
            Pull_PTGE.extend(['PayablesToGroupEnterprises', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS OTHER DEBT RAISED BY ISSUANCE OF BONDS
        for z in range(0, len(Facts)):
            # checks if ODRB is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_ODRB, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_ODRB.extend(['OtherDebtRaisedByIssuanceOfBonds', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_ODRB, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_ODRB.extend(['OtherDebtRaisedByIssuanceOfBonds',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "OtherDebtRaisedByIssuanceOfBonds" not in Pull_ODRB:
            Pull_ODRB.extend(['OtherDebtRaisedByIssuanceOfBonds', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS PREPAYMENTS OF WORK IN PROGRESS
        for z in range(0, len(Facts)):
            # checks if POWIP is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_POWIP, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_POWIP.extend(['PrepaymentsOfWorkInProgress', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_POWIP, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_POWIP.extend(['PrepaymentsOfWorkInProgress',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "PrepaymentsOfWorkInProgress" not in Pull_POWIP:
            Pull_POWIP.extend(['PrepaymentsOfWorkInProgress', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

    # GETS CONTRACT WORK IN PROGRESS
        for z in range(0, len(Facts)):
            # checks if Contract Work In Progress is in the name of the chunk, if it's a Group company (koncern) and if the no. belongs to the correct period
            if bool(re.search(pattern_CWIP, Facts[z]["oim:concept"])) is True and len(Facts[z]["explicitDimensions"]) is 1 and bool(re.search(pattern_Consolidated, Facts[z]["explicitDimensions"][0]["dimension"])) is True \
                    and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                # checks if the entry 'precision' exists
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_CWIP.extend(['ContractWorkInProgress', x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            elif bool(re.search(pattern_CWIP, Facts[z]["oim:concept"])) is True and Facts[z]["explicitDimensions"] == [] and str(Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"]) in str(Facts[z]["oim:period"]):
                if "precision" in str(Facts[z]):
                    if Facts[z]["precision"] == " ":
                        prec = 'ingen'
                    else:
                        prec = str(Facts[z]["precision"])
                else:
                    prec = 'eksisterer ikke'
                Pull_CWIP.extend(['ContractWorkInProgress',  x, Facts[z]["value"], prec, Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], 0, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])
            else:
                pass
        if "ContractWorkInProgress" not in Pull_CWIP:
            Pull_CWIP.extend(['ContractWorkInProgress', x, '-999999999999999', 'ukendt', Facts[z]["oim:period"], Data[y]["_id"], Data[y]["_source"]["Report"]["metadata"]["PeriodeSlut"], -1, datetime.strptime(Data[y]["_source"]["Report"]["externalReferences"]["OffentliggoerelsesTidspunkt"][:10], "%Y-%m-%d").date()])

ProfitLoss = list(chunkylonky(Pull_PL, 9))
Assets = list(chunkylonky(Pull_Assets, 9))
NCAssets = list(chunkylonky(Pull_NCAssets, 9))
IntAssets = list(chunkylonky(Pull_IntAssets, 9))
PPE = list(chunkylonky(Pull_PPE, 9))
LB = list(chunkylonky(Pull_LB, 9))
IP = list(chunkylonky(Pull_IP, 9))
LIAR = list(chunkylonky(Pull_LIAR, 9))
CA = list(chunkylonky(Pull_CA, 9))
Inventories = list(chunkylonky(Pull_Inventories, 9))
SR = list(chunkylonky(Pull_SR, 9))
SI = list(chunkylonky(Pull_SI, 9))
CACE = list(chunkylonky(Pull_CACE, 9))
Equity = list(chunkylonky(Pull_Equity, 9))
LOTP = list(chunkylonky(Pull_LOTP, 9))
LLOTP = list(chunkylonky(Pull_LLOTP, 9))
SLOTP = list(chunkylonky(Pull_SLOTP, 9))
Provisions = list(chunkylonky(Pull_Provisions, 9))
CDTA = list(chunkylonky(Pull_CDTA, 9))
RR = list(chunkylonky(Pull_RR, 9))
DTB = list(chunkylonky(Pull_DTB, 9))
MD = list(chunkylonky(Pull_MD, 9))
PTGE = list(chunkylonky(Pull_PTGE, 9))
ODRB = list(chunkylonky(Pull_ODRB, 9))
POWIP = list(chunkylonky(Pull_POWIP, 9))
CWIP = list(chunkylonky(Pull_CWIP, 9))


# Cleanses data for dublicates
ProfitLoss.sort()
ProfitLossCleaned = [ProfitLoss[i] for i in range(len(ProfitLoss)) if ProfitLoss == 0 or ProfitLoss[i] != ProfitLoss[i-1]]
Assets.sort()
AssetsCleaned = [Assets[i] for i in range(len(Assets)) if Assets == 0 or Assets[i] != Assets[i-1]]
NCAssets.sort()
NCAssetsCleaned = [NCAssets[i] for i in range(len(NCAssets)) if NCAssets == 0 or NCAssets[i] != NCAssets[i-1]]
IntAssets.sort()
IntAssetsCleaned = [IntAssets[i] for i in range(len(IntAssets)) if IntAssets == 0 or IntAssets[i] != IntAssets[i-1]]
PPE.sort()
PPECleaned = [PPE[i] for i in range(len(PPE)) if PPE == 0 or PPE[i] != PPE[i-1]]
LB.sort()
LBCleaned = [LB[i] for i in range(len(LB)) if LB == 0 or LB[i] != LB[i-1]]
IP.sort()
IPCleaned = [IP[i] for i in range(len(IP)) if IP == 0 or IP[i] != IP[i-1]]
LIAR.sort()
LIARCleaned = [LIAR[i] for i in range(len(LIAR)) if LIAR == 0 or LIAR[i] != LIAR[i-1]]
CA.sort()
CACleaned = [CA[i] for i in range(len(CA)) if CA == 0 or CA[i] != CA[i-1]]
Inventories.sort()
InventoriesCleaned = [Inventories[i] for i in range(len(Inventories)) if Inventories == 0 or Inventories[i] != Inventories[i-1]]
SR.sort()
SRCleaned = [SR[i] for i in range(len(SR)) if SR == 0 or SR[i] != SR[i-1]]
SI.sort()
SICleaned = [SI[i] for i in range(len(SI)) if SI == 0 or SI[i] != SI[i-1]]
CACE.sort()
CACECleaned = [CACE[i] for i in range(len(CACE)) if CACE == 0 or CACE[i] != CACE[i-1]]
Equity.sort()
EquityCleaned = [Equity[i] for i in range(len(Equity)) if Equity == 0 or Equity[i] != Equity[i-1]]
LOTP.sort()
LOTPCleaned = [LOTP[i] for i in range(len(LOTP)) if LOTP == 0 or LOTP[i] != LOTP[i-1]]
LLOTP.sort()
LLOTPCleaned = [LLOTP[i] for i in range(len(LLOTP)) if LLOTP == 0 or LLOTP[i] != LLOTP[i-1]]
SLOTP.sort()
SLOTPCleaned = [SLOTP[i] for i in range(len(SLOTP)) if SLOTP == 0 or SLOTP[i] != SLOTP[i-1]]
Provisions.sort()
ProvisionsCleaned = [Provisions[i] for i in range(len(Provisions)) if Provisions == 0 or Provisions[i] != Provisions[i-1]]
CDTA.sort()
CDTACleaned = [CDTA[i] for i in range(len(CDTA)) if CDTA == 0 or CDTA[i] != CDTA[i-1]]
RR.sort()
RRCleaned = [RR[i] for i in range(len(RR)) if RR == 0 or RR[i] != RR[i-1]]
DTB.sort()
DTBCleaned = [DTB[i] for i in range(len(DTB)) if DTB == 0 or DTB[i] != DTB[i-1]]
MD.sort()
MDCleaned = [MD[i] for i in range(len(MD)) if MD == 0 or MD[i] != MD[i-1]]
PTGE.sort()
PTGECleaned = [PTGE[i] for i in range(len(PTGE)) if PTGE == 0 or PTGE[i] != PTGE[i-1]]
ODRB.sort()
ODRBCleaned = [ODRB[i] for i in range(len(ODRB)) if ODRB == 0 or ODRB[i] != ODRB[i-1]]
POWIP.sort()
POWIPCleaned = [POWIP[i] for i in range(len(POWIP)) if POWIP == 0 or POWIP[i] != POWIP[i-1]]
CWIP.sort()
CWIPCleaned = [CWIP[i] for i in range(len(CWIP)) if CWIP == 0 or CWIP[i] != CWIP[i-1]]

# One chunk
ProfitLossCleaned.extend(AssetsCleaned + NCAssetsCleaned + IntAssetsCleaned + PPECleaned + LBCleaned + IPCleaned
                         + LIARCleaned + CACleaned + InventoriesCleaned + SRCleaned + CACECleaned + EquityCleaned
                         + LOTPCleaned + LLOTPCleaned + SLOTPCleaned + ProvisionsCleaned + CDTACleaned + RRCleaned
                         + DTBCleaned + MDCleaned + PTGECleaned + ODRBCleaned + POWIPCleaned + CWIPCleaned)

# Print to csv file
headers = ["Regnskabspost", "CVR", "Value", "Precision", "Dato", "Report_Id", "PeriodeSlut", "Koncern", "Off.tidspunkt"]
ProfitLossDF = pd.DataFrame(ProfitLossCleaned, columns=headers)

ProfitLossDF.to_csv('AccountingNumbers.csv', sep=';', encoding='utf-8', index=False)


