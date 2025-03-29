import boto3
import json
import uuid
import time
import os
import logging
from botocore.exceptions import ClientError

from connectionsinsights.neptune import (
    getOrCreateID,
    GraphConnect
)

# Configure logging
logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

dynamodb = boto3.resource('dynamodb')
dynamodb_table_name = os.environ["DDBTBL_INGESTION"]
table = dynamodb.Table(dynamodb_table_name)

def lambda_handler(event, context):
    try:
        # Log input event (excluding sensitive data)
        logger.info(f"Processing event with {len(event.get('output', []))} output items")
        
        # Validate input
        if not event.get("Summary") or not event.get("output"):
            logger.error("Missing required fields in event")
            raise ValueError("Missing required fields in event: Summary or output")

        summary = event["Summary"]
        main_entity_name = summary["MAIN_ENTITY"]["NAME"]
        attributes = summary["MAIN_ENTITY"]["ATTRIBUTES"]
        
        logger.info(f"Processing main entity: {main_entity_name}")
        logger.debug(f"Main entity attributes: {json.dumps(attributes)}")

        allEdges = []
        results = {}

        # Process each output item
        for obj in event["output"]:
            try:
                item_id = obj[list(obj.keys())[0]]
                logger.debug(f"Fetching DynamoDB item with ID: {item_id}")
                
                response = table.get_item(Key={'id': item_id})
                
                if 'Item' not in response:
                    logger.warning(f"No item found for ID: {item_id}")
                    continue

                data = json.loads(response["Item"]["data"])
                logger.debug(f"Successfully parsed data for ID: {item_id}")
                
                # Process each key in data
                for key in data:
                    if not key:
                        logger.debug("Skipping empty key")
                        continue
                    
                    # Group records into A-Z,#
                    first_char = key[0].upper() if key[0].isalpha() else "#"
                    results.setdefault(first_char, []).append({key: data[key]})
                    
                    # Process edges based on type
                    entity_type = data[key].get("TYPE")
                    if entity_type:
                        logger.debug(f"Processing edge for entity type: {entity_type}")
                        edge = create_edge(key, data[key], main_entity_name, entity_type)
                        if edge:
                            allEdges.append(edge)
                            logger.debug(f"Created edge: {edge}")

            except ClientError as e:
                logger.error(f"DynamoDB error for item {item_id}: {str(e)}")
                continue
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for item {item_id}: {str(e)}")
                continue

        # Create main entity in Neptune
        try:
            logger.info("Establishing Neptune connection")
            g, connection = GraphConnect()
            
            # Process attributes
            logger.debug("Processing main entity attributes")
            processed_attributes = []
            for attribute in attributes:
                for key, value in attribute.items():
                    if isinstance(value, list):
                        attribute[key] = ",".join(value)
                processed_attributes.append(attribute)

            logger.info("Creating/updating main entity in Neptune")
            main_entity_id = getOrCreateID(g, "COMPANY", main_entity_name, processed_attributes, allEdges)
            logger.info(f"Main entity created/updated with ID: {main_entity_id}")
            
            connection.close()
            logger.debug("Neptune connection closed")
            
        except Exception as e:
            logger.error(f"Neptune operation failed: {str(e)}", exc_info=True)
            raise

        # Store results in DynamoDB
        logger.info(f"Storing results for {len(results)} groups")
        uuids = []
        for key in results:
            try:
                id = str(uuid.uuid4())
                logger.debug(f"Storing group {key} with ID: {id}")
                
                item = {
                    "id": id,
                    "main_entity": main_entity_name,
                    "key": key,
                    "data": json.dumps(results[key]),
                    "summary": json.dumps(summary),
                    "main_entity_all_edges": json.dumps(allEdges),
                    "main_entity_id": main_entity_id,
                    'ttl_timestamp': int(time.time()) + 7200
                }
                
                table.put_item(Item=item)
                uuids.append(id)
                logger.debug(f"Successfully stored group {key}")
                
            except ClientError as e:
                logger.error(f"Failed to store results for key {key}: {str(e)}")
                continue

        logger.info(f"Successfully processed {len(uuids)} groups")
        return uuids

    except Exception as e:
        logger.error(f"Fatal error in lambda_handler: {str(e)}", exc_info=True)
        raise

def create_edge(key, data, main_entity_name, entity_type):
    """Helper function to create edge based on entity type"""
    logger.debug(f"Creating edge for {key} of type {entity_type}")
    
    edge_mappings = {
        "CUSTOMER": (lambda: f"{key} is a customer of (PRODUCTS_USED:{','.join(data['PRODUCTS_USED'])}) {main_entity_name}"),
        "SUPPLIER": (lambda: f"{key} is a supplier of (RELATIONSHIP:{','.join(data['RELATIONSHIP'])}) {main_entity_name}"),
        "COMPETITOR": (lambda: f"{key} is a competitor of (COMPETING_IN:{','.join(data['COMPETING_IN'])}) {main_entity_name}"),
        "DIRECTOR": (lambda: f"{key} is a director of (ROLE: {','.join(data['ROLE'])}) {main_entity_name}")
    }
    
    edge_creator = edge_mappings.get(entity_type)
    if not edge_creator:
        logger.warning(f"Unknown entity type: {entity_type}")
        return None
        
    return edge_creator()
