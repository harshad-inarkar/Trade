import pickle, sys

from app import PKL_DIR

def compare_pickle_files(file1_path, file2_path):
    try:
        with open(file1_path, 'rb') as f1, open(file2_path, 'rb') as f2:
            obj1 = pickle.load(f1)
            obj2 = pickle.load(f2)
        
            # Deep equality check
            # diffs = find_diffs(obj1, obj2)
            # for d in diffs:
            #     print(d)
            
            diffs = tolerant_diff(obj1, obj2, tolerance=0.01)  # Adjust for 2 decimals

            print(f'No of diffs: {len(diffs)}')
            for d in diffs[:20]:
                print(d)


            return obj1 == obj2 
    
    except Exception as e:
        print(f"Error loading files: {e}")
        return False



def find_diffs(obj1, obj2, path=""):
    diffs = []
    if obj1 == obj2:
        return diffs
    
    if isinstance(obj1, dict) and isinstance(obj2, dict):
        for key in set(obj1) | set(obj2):
            new_path = f"{path}.{key}" if path else key
            diffs.extend(find_diffs(obj1.get(key), obj2.get(key), new_path))
    
    elif isinstance(obj1, (list, tuple)) and isinstance(obj2, (list, tuple)):
        min_len = min(len(obj1), len(obj2))
        for i in range(min_len):
            diffs.extend(find_diffs(obj1[i], obj2[i], f"{path}[{i}]"))
        if len(obj1) != len(obj2):
            diffs.append(f"Length diff at {path}: {len(obj1)} vs {len(obj2)}")
    
    else:
        diffs.append(f"{path}: {obj1} != {obj2}")
    
    return diffs


def float_equal(a, b, tolerance=0.01):  # 2 decimal places: 0.01
    return abs(a - b) < tolerance

def tolerant_diff(obj1, obj2, path="", tolerance=0.01):
    diffs = []
    if isinstance(obj1, dict) and isinstance(obj2, dict):
        for key in set(obj1) | set(obj2):
            new_path = f"{path}.{key}" if path else key
            diffs.extend(tolerant_diff(obj1.get(key, None), obj2.get(key, None), new_path, tolerance))
    elif isinstance(obj1, (list, tuple)) and isinstance(obj2, (list, tuple)):
        min_len = min(len(obj1), len(obj2))
        for i in range(min_len):
            diffs.extend(tolerant_diff(obj1[i], obj2[i], f"{path}[{i}]", tolerance))
        if len(obj1) != len(obj2):
            diffs.append(f"Length mismatch at {path}  {len(obj1)}  {len(obj2)}\n{obj1}\n{obj2}")
    elif isinstance(obj1, (int, float)) and isinstance(obj2, (int, float)):
        if not float_equal(obj1, obj2, tolerance):
            diffs.append(f"{path}: {obj1} != {obj2} (diff={abs(obj1-obj2):.4f})")
    elif obj1 != obj2:
        diffs.append(f"{path}: {obj1} != {obj2}")
    
    return diffs



if __name__ == '__main__':
    if not (len(sys.argv) == 3):
        print("Usage: python script.py pkl1 pkl2")
        sys.exit(1)

    if len(sys.argv) == 3:
        pkl1 = f'{PKL_DIR}/{sys.argv[1]}'
        pkl2 = f'{PKL_DIR}/{sys.argv[2]}'

        is_same = compare_pickle_files(pkl1, pkl2)

        sd= 'same' if is_same else 'different'

        print(f"Ddta Files {pkl1} and {pkl2} are {sd}")

