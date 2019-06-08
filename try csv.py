import csv

path_to_read = r"E:\_SCRIPTS\_sandbox\TB_data_dictionary_2019-06-08.csv"
path_to_write = r"E:\_SCRIPTS\_sandbox\data.csv"


def csv_reader(file_path):
    """
    Read a csv file
    """
    try:
        with open(file_path, "r") as f_obj:
            reader = csv.reader(f_obj)
            res = ''
            for row in reader:
                res += " ".join(row) + '\n'
    except IOError as err:
        print(err)
    return res


def save_data(file_path, txt):
    try:
        with open(file_path, 'w+') as save_file:
            save_file.write(txt)
    except IOError as err:
        print(err)


def get_file_object(file_path):
    try:
        with open(file_path, "r") as f_obj:
            return f_obj
    except IOError as err:
        print(err)


def csv_dict_reader(file_path):
    """
    Read a CSV file using csv.DictReader
    """
    with open(file_path, 'r') as file_obj:

        reader = csv.DictReader(file_obj, delimiter=',')
        for line in reader:
            #print(line["variable_name "]),
            print(line["dataset"])

if __name__ == "__main__":
    res = csv_reader(path_to_read)
    print(len(res))
    #save_data(path_to_write, res)
    csv_dict_reader(path_to_write)