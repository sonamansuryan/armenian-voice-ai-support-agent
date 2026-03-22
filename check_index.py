import chromadb
client = chromadb.PersistentClient(path='chroma_db')
col = client.get_collection('bank_knowledge')
results = col.get(
    where={"$and": [{"bank": {"$eq": "Ardshinbank"}}, {"section": {"$eq": "branches"}}]},
    include=['documents']
)
gyumri = [d for d in results['documents'] if 'Գյումրի' in d]
print(f'Gyumri chunks in index: {len(gyumri)}')
for g in gyumri:
    print(g[:100])