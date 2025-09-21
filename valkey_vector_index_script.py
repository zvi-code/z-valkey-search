#!/usr/bin/env python3
"""
Simple script to create a vector index in Valkey and ingest one vector document.
Connects to local Valkey server on default port (6379).
"""

import struct
from valkey import Valkey, ResponseError


def create_vector_blob(vector):
    """
    Convert a list of floats to a binary blob format expected by Valkey Search.
    
    Args:
        vector: List of float values representing the vector
        
    Returns:
        Binary blob representation of the vector
    """
    # Pack floats as little-endian 32-bit floats
    return struct.pack(f'<{len(vector)}f', *vector)


def main():
    # Connect to local Valkey server
    client = Valkey(host='localhost', port=6379, decode_responses=False)
    
    try:
        # Check connection
        client.ping()
        print("Connected to Valkey server")
        
        # Define index parameters
        index_name = "my_vector_index"
        vector_dim = 128
        
        # Create the vector index
        # FT.CREATE index_name ON HASH PREFIX 1 prefix SCHEMA field_name VECTOR algorithm num_params [param_name param_value ...]
        create_cmd = [
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "description", "TEXT", 
            "embedding", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32",
            "DIM", str(vector_dim),
            "DISTANCE_METRIC", "COSINE"
        ]
        
        result = client.execute_command(*create_cmd)
        print(f"Index created: {result}")
        
        # Create a sample vector (normalized for cosine similarity)
        # Using a simple pattern for demonstration
        sample_vector = [0.1 * (i % 10) for i in range(vector_dim)]
        
        # Normalize the vector for cosine similarity
        magnitude = sum(x**2 for x in sample_vector) ** 0.5
        normalized_vector = [x / magnitude for x in sample_vector]
        
        # Convert vector to binary blob
        vector_blob = create_vector_blob(normalized_vector)
        
        # Insert a document with vector
        doc_id = "doc:1"
        client.hset(doc_id, mapping={
            "title": "Sample Document",
            "description": "This is a test document with an embedding vector",
            "embedding": vector_blob
        })
        print(f"Document inserted: {doc_id}")
        
        # Verify the document was indexed
        info_result = client.execute_command("FT.INFO", index_name)
        
        # Parse info result to find number of indexed documents
        info_dict = {}
        for i in range(0, len(info_result), 2):
            if i + 1 < len(info_result):
                key = info_result[i].decode() if isinstance(info_result[i], bytes) else str(info_result[i])
                value = info_result[i + 1]
                info_dict[key] = value
        
        num_docs = info_dict.get('num_docs', 0)
        print(f"Number of indexed documents: {num_docs}")
        
        # Perform a simple vector search to verify
        search_vector = normalized_vector  # Use same vector for search
        search_blob = create_vector_blob(search_vector)
        
        search_result = client.execute_command(
            "FT.SEARCH", index_name,
            "*=>[KNN 1 @embedding $vec_param]",
            "PARAMS", "2", "vec_param", search_blob,
            "RETURN", "3", "title", "description", "__embedding_score"
        )
        
        print(f"\nSearch result: Found {search_result[0]} documents")
        if search_result[0] > 0:
            doc_key = search_result[1]
            doc_fields = search_result[2]
            print(f"Document ID: {doc_key}")
            for i in range(0, len(doc_fields), 2):
                field_name = doc_fields[i].decode() if isinstance(doc_fields[i], bytes) else doc_fields[i]
                field_value = doc_fields[i + 1].decode() if isinstance(doc_fields[i + 1], bytes) else doc_fields[i + 1]
                print(f"  {field_name}: {field_value}")
        
    except ResponseError as e:
        print(f"Valkey error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
        print("\nDisconnected from Valkey server")


if __name__ == "__main__":
    main()
