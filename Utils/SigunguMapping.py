import json


def sigungu_mapping(sigungu_name: str):
    with open("../Data/SigunguMapping.json", "r") as f:
        sigungu_mapping_data = json.load(f)
    
    
    for sido, sigungu_dict in sigungu_mapping_data.items():
        for code, name in sigungu_dict.items():
            if name == sigungu_name:
                return code
    
    return None  
    


