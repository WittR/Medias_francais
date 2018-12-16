# modelsBDD.py
import csv
from neo4j import *

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "root"))


def nodesFromTSV():
    with open('medias_francais.tsv', encoding='utf-8') as tsvfile:
        tx = driver.session().begin_transaction()
        reader = csv.DictReader(tsvfile, delimiter='\t')

        for row in reader:
            if row['typeCode'] == '1':
                statement = "CREATE (a:PersonePhysique {nom:{nom}, rangChallenges:{rangChallenges}, " \
                            "commentaire:{commentaire}})"
                dict = {}
                for x in row:
                    dict[x] = row[x]
                tx.run(statement, dict)


            elif row['typeCode'] == '2':
                statement = "CREATE (a:PersoneMorale {nom:{nom}, commentaire:{commentaire}})"
                dict = {}
                for x in row:
                    dict[x] = row[x]
                tx.run(statement, dict)
            elif row['typeCode'] == '3':
                statement = "CREATE (a:Media {nom:{nom},mediaType:{mediaType}, mediaPeriodicite:{mediaPeriodicite}, " \
                            " mediaEchelle:{mediaEchelle}, commentaire:{commentaire}})"
                dict = {}
                for x in row:
                    dict[x] = row[x]
                tx.run(statement, dict)
            elif row['typeCode'] == '4':
                statement = "CREATE (a:Etat {nom:{nom}, commentaire:{commentaire}})"
                dict = {}
                for x in row:
                    dict[x] = row[x]
                tx.run(statement, dict)
        tx.commit()


def edgesFromTSV():
    with open('relations_medias_francais.tsv', encoding='utf-8') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        tx = driver.session().begin_transaction()
        statement = "MATCH (n1 {nom:{origine}}), (n2 {nom:{cible}}) CREATE(n1) - [r:détient]->(n2) " \
                    "SET r = {valeur: {valeur},source: {source}, datePublication: {datePublication}," \
                    " dateConsultation: {dateConsultation}} "

        for row in reader:
            dict = {}
            for x in row:
                dict[x] = row[x]
            tx.run(statement, dict)
        tx.commit()


# nodesFromTSV()
# edgesFromTSV()
def exportRelation():
    tx = driver.session().begin_transaction()
    result = tx.run("MATCH (n1)-[r]->(n2) RETURN r")
    list_dic = []
    for x in result:
        dic = {}
        for y in x:
            dic['id'] = y.id
            dic['origine'] = y.nodes[0].id
            dic['cible'] = y.nodes[1].id
            dic['source'] = y['source']
            dic['valeur'] = y['valeur']
            dic['datePublication'] = y['datePublication']
            dic['dateConsultation'] = y['dateConsultation']
            list_dic.append(dic)

    with open('relations_medias_francais2.tsv', 'w') as f:
        w = csv.DictWriter(f, list_dic[0].keys(), delimiter='\t', lineterminator='\n')
        w.writeheader()
        w.writerows(list_dic)


tx = driver.session().begin_transaction()
result = tx.run("MATCH (n) RETURN n")
list_dic = []
for x in result:
    dic = {}
    for y in x:
        dic['id'] = y.id
        for z in y.labels:
            dic['typeLibelle'] = z
        for z in y:
            dic[z] = y[z]
    list_dic.append(dic)
print(list_dic)
keys = ['id', 'nom', 'typeLibelle', 'rangChallenges', 'commentaire', 'mediaType', 'mediaPeriodicite', 'mediaEchelle']
with open('medias_francais2.tsv', 'w', encoding='utf-8') as f:
    w = csv.DictWriter(f, keys, delimiter='\t', lineterminator='\n')
    w.writeheader()
    w.writerows(list_dic)
