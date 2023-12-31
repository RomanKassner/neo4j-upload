import os
from datetime import datetime

from neo4j import GraphDatabase
from py2neo import Graph
from pathvalidate import sanitize_filepath
import pm4py
import pandas as pd


# Create the map for abbreviations used in the SAP VBFA table's VEBLN field to their entire names.
# This might needs to be altered/replace when one wants to upload another SAP table.
vbtypn_map = {'A': 'Inquiry',
              'B': 'Quotation',
              'C': 'Order',
              'D': 'Item proposal',
              'E': 'Scheduling agreement',
              'F': 'Scheduling agreement with external service agent',
              'G': 'Contract',
              'H': 'Returns',
              'I': 'Order w/o charge',
              'J': 'Delivery',
              'K': 'Credit memo request',
              'L': 'Debit memo request',
              'M': 'Invoice',
              'N': 'Invoice cancellation',
              'O': 'Credit memo',
              'P': 'Debit memo',
              'Q': 'WMS transfer order',
              'R': 'Goods movement',
              'S': 'Credit memo cancellation',
              'T': 'Returns delivery for order',
              'U': 'Pro forma invoice',
              'V': 'Purchase Order',
              'W': 'Independent reqts plan',
              'X': 'Handling unit',
              '0': 'Master contract',
              '1': 'Sales activities (CAS)',
              '2': 'External transaction',
              '3': 'Invoice list',
              '4': 'Credit memo list',
              '5': 'Intercompany invoice',
              '6': 'Intercompany credit memo',
              '7': 'Delivery/shipping notification',
              '8': 'Shipment',
              'a': 'Shipment costs',
              'b': 'CRMO pportunity',
              'c': 'Unverified delivery',
              'd': 'Trading Contract',
              'e': 'Allocation table',
              'f': 'Additional Billing Documents',
              'g': 'Rough Goods Receipt (onlyIS-Retail)',
              'h': 'Cancel Goods Issue',
              'i': 'Goods receipt',
              'j': 'JIT call',
              'n': 'Reserved',
              'o': 'Reserved',
              'p': 'GoodsMovement(Documentation)',
              'q': 'Reserved',
              'r': 'TD Transport (onlyIS-Oil)',
              's': 'Load Confirmation, Reposting (OnlyIS-Oil)',
              't': 'Gain / Loss (OnlyIS-Oil)',
              'u': 'Reentry into Storage (OnlyIS-Oil)',
              'v': 'Data Collation (onlyIS-Oil)',
              'w': 'Reservation (OnlyIS-Oil)',
              'x': 'Load Confirmation, Goods Receipt (OnlyIS-Oil)',
              '$': '(AFS)',
              '+': 'Accounting Document (Temporary)',
              '-': 'Accounting Document (Temporary)',
              '#': 'Revenue Recognition (Temporary)',
              '~': 'Revenue Cancellation (Temporary)',
              '�': 'Revenue Recognition / New View (Temporary)',
              'NULL': 'Revenue Cancellation / NewView (Temporary)',
              ':': 'Service Order',
              '.': 'Service Notification',
              '&': 'Warehouse Document',
              '*': 'Pick Order',
              ',': 'Shipment Document',
              '^': 'Reserved',
              '|': 'Reserved',
              'k': 'Agency Document'}


def upload_vbfa(path, clear):
    # Establish DB connection with Neo4j
    db_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "12345678"))
    session = db_connection.session()
    print("Established DB Connection!")

    # Clear database if needed
    if clear:
        session.run("CALL apoc.periodic.iterate("
                    " 'MATCH (n) RETURN n', "
                    " 'DETACH DELETE n', "
                    "{batchSize: 10000, parallel: true})"
                    )
        for index in session.run("SHOW INDEXES"):
            session.run("DROP INDEX " + index[1])
        print("Cleaned DB!")

    # Read SAP table
    sap_table = pd.read_parquet(path)
    print("Read SAP table!")

    # Create indices to speed upload/merge query up
    if clear:
        session.run("CREATE INDEX d_num_dex FOR (d:Document) ON (d.d_num)")
        session.run("CREATE INDEX d_type_dex FOR (d:Document) ON (d.d_type)")

    vbeln = sap_table.get("VBELN")
    mandt = sap_table.get("MANDT")
    vbtyp_n = sap_table.get("VBTYP_N")
    posn_v = sap_table.get("POSNV")
    posn_n = sap_table.get("POSNN")
    erdat = sap_table.get("ERDAT")
    erzet = sap_table.get("ERZET")
    vbelv = sap_table.get("VBELV")
    vbtyp_v = sap_table.get("VBTYP_V")

    vbeln_list = list(vbeln.values)

    for i in range(0, len(vbeln)):
        # Subsequent sales and distribution document
        doc_num = vbeln[i]

        # Client
        client = mandt[i]

        # Document category of subsequent document
        d_type = vbtyp_n[i]
        doc_type = vbtypn_map.get(d_type, d_type)

        if doc_type == " " or doc_type == "l" or d_type == "l":
            print("DOC")
            print("\" " + d_type + "\" ", "\" " + doc_type + "\" ")

        # Preceding + Subsequent  item of an SD document
        pos_prev = posn_v[i]
        pos_sub = posn_n[i]

        # Upload time of subsequent document
        upload_date_time = pd.to_datetime(erdat[i] + erzet[i])

        # Preceding sales and distribution document
        prev_num = vbelv[i]

        # Document category of preceding SD document
        p_type = vbtyp_v[i]
        prev_type = vbtypn_map.get(p_type, p_type)

        # Preceding client and upload time
        if prev_num is not None and prev_num in vbeln_list:
            prev_index = vbeln_list.index(prev_num)

            prev_client = mandt[prev_index]
        else:
            prev_client = "N/A"

        # Create or find subsequent document, then create or find previous document. Then create relation between the two.
        session.run("MERGE(d:Document {d_num: $doc_num, d_type: $doc_type}) "
                    "ON CREATE "
                    "SET d.d_num = $doc_num, d.d_type = $doc_type, d.client = $client"
                    " "
                    "MERGE(prev:Document {d_num: $prev_num, d_type: $prev_type}) "
                    "ON CREATE "
                    " SET prev.d_num = $prev_num,  prev.d_type = $prev_type, prev.client = $client"
                    " "
                    "MERGE (prev)-[:PREVIOUS_DOC_OF {from: $prev_type, to: $doc_type, created: $upload_date_time, "
                    "sub_item: $pos_sub, prec_item: $pos_prev}]->(d) "
                    , doc_num=doc_num, doc_type=doc_type, prev_num=prev_num, prev_type=prev_type, client=client
                    , upload_date_time=upload_date_time, prev_client=prev_client,
                    pos_sub=pos_sub, pos_prev=pos_prev)

        # The following are suggestions for queries that create nodes for each item and client. Note that this creates
        # a tremendous amount of relationships. Therefore, we avoided creating such nodes and put the information into
        # each event-to-object relationship.

        # Creates individual client nodes
        # session.run("MERGE(d:Document {d_num: $doc_num, d_type: $doc_type}) "
        #             "WITH d "
        #             "MERGE (c:Client {c_num: $c_num}) "
        #             "ON CREATE "
        #             " SET c.c_num = $c_num "
        #             "WITH d, c "
        #             "MERGE (d)-[:CLIENT]->(c)",
        #             c_num=client, doc_num=doc_num, doc_type=doc_type)

        # Creates individual item nodes
        # session.run("MERGE(d:Document {d_num: $doc_num, d_type: $doc_type, d_client: $client}) "
        #             "WITH d "
        #             "MERGE(pos_prev:Item {pos_num: $pos_prev}) "
        #             "ON CREATE "
        #             " SET pos_prev.d_num = $pos_prev "
        #             "WITH d, pos_prev "
        #             "MERGE (pos_prev)-[r:ITEM_BEFORE]->(d) "
        #             , doc_num=doc_num, doc_type=doc_type, pos_prev=pos_prev, client=client)
        #
        # session.run("MERGE(d:Document {d_num: $doc_num, d_type: $doc_type, d_client: $client}) "
        #             "WITH d "
        #             "MERGE(pos_sub:Item {pos_num: $pos_sub}) "
        #             "ON CREATE "
        #             " SET pos_sub.d_num = $pos_sub "
        #             "WITH d, pos_sub "
        #             "MERGE (pos_sub)-[r:ITEM_AFTER]->(d) "
        #             , doc_num=doc_num, doc_type=doc_type, pos_sub=pos_sub, client=client)


def upload_ocel(path, clear):
    # Establish DB connection with Neo4j
    db_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "12345678"))
    session = db_connection.session()
    print("Established DB Connection!")

    # Clear data base if specified
    if clear:
        session.run("MATCH(n)"
                    "DETACH DELETE(n)")

        for index in session.run("SHOW INDEXES"):
            session.run("DROP INDEX " + index[1])
        print("Cleaned DB!")

    # Read OCEL log from local file
    log = pm4py.read_ocel(path)
    print("Read OCEL log!")

    if clear:
        session.run("CREATE INDEX e_index FOR (e:Event) ON (e.e_id)")
        session.run("CREATE INDEX o_index FOR (o:Object) ON (o.obj_id)")

    # Create Event Nodes
    for _, event in log.events.iterrows():
        date = datetime.strptime(str(event[2]), '%Y-%m-%d %H:%M:%S')
        session.run("CREATE(e:Event {e_id: $e_id, activity: $activity, timestamp: $stamp})"
                    , e_id=event[0], activity=event[1], stamp=date)
        # , vmap=str(event[3]) + " " + str(event[4]) if not transfer_log
    print("Created Event Nodes!")

    # Create Object Nodes
    for _, obj in log.objects.iterrows():
        session.run("CREATE(o:Object {obj_id: $obj_id, obj_attribute: $obj_at})", obj_id=obj[0], obj_at=obj[1])
    print("Created Object Nodes!")

    # Create Relations
    for _, relation in log.relations.iterrows():
        session.run("MATCH (e:Event {e_id: $e_id}), (o:Object {obj_id: $obj_id}) "
                    "CREATE (o)-[r:part_of {name: e.e_id + ' <-> ' + o.obj_id}]->(e)"
                    , e_id=relation[0], obj_id=relation[3])

    db_connection.close()


if __name__ == '__main__':
    # Here one (or both) functions are called. The paths have to be appropriately changed. One can set the parameter
    # clear in both functions to clear the database, before uploading from the specified path.

    try:
        graph = Graph(uri="bolt://localhost:7687", auth=("neo4j", "12345678"))
        choice = input('Do you wish to clean your current database before uploading?\n')
        if choice.upper() in ["YES", "Y"]:
            choice = True
        elif choice.upper() in ["NO", "N"]:
            choice = False
        else:
            print("Not a valid choice! Please try again.")
            exit()

    except Exception:
        print('No instance of Neo4j found! Please ensure the Neo4j database is running and try again.')
        exit()

    path = input("Do you wish to upload an OCEL or a SAP log to the current Neo4j instance? "
                 "Type OCEL or SAP:\n")

    if path.upper() == "OCEL":
        path_ocel = input("Please provide the path of your OCEL log:\n")
        if not os.path.exists(path_ocel):
            print("Path not found!")
            exit()
        else:
            upload_ocel(path_ocel, choice)
    elif path.upper() == "SAP":
        path_sap = input("Please provide the path of your SAP table:\n")
        if not os.path.exists(path_sap):
            print("Path not found!")
            exit()
        else:
            upload_vbfa(path_sap, choice)
    else:
        print("Invalid choice! Only OCEL and SAP are available. Please try again!")
        exit()