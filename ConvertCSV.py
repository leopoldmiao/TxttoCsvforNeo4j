import csv
from os import sep
import pandas as pd
import time

def toNodeCSV(txt_path,csv_path_node):
    # 读取文件路径与目的文件路径
    txt_path = txt_path
    csv_path_node = csv_path_node

    #首先设置文件头
    with open(csv_path_node, 'w')  as csvfile_node:
        header = ['userid:ID',':LABEL']
        writer = csv.writer(csvfile_node)
        writer.writerow(header)
    csvfile_node.close()

    #读取文件， error_bad_lines可能是跳过报错行,header=None是说明txt文件中没有表头。sep='\s*'表示有多个空格作为间隔符
    #reader = pd.read_table(txt_path, iterator=True, encoding='utf8', error_bad_lines=False)
    reader = pd.read_table(txt_path, header=None, sep='\s*',iterator=True, encoding='utf8', error_bad_lines=False)
    #循环读取
    loop = True
    while loop:
        try:
            #每次读取5000个
            chunk = reader.get_chunk(50000)
            #print(chunk[0])
            #print(chunk[1])
            s1=chunk[0]
            s2=chunk[1]
            print("=====")
            chunk_new = pd.DataFrame()
            chunk_new[0] = pd.concat([s1,s2],axis=0)
            chunk_new[1]="USER"
            #print(chunk_new)
            #mode='a' 追加读取, index=False 舍去行数
            chunk_new.to_csv(csv_path_node, mode='a', index=False, header=None, encoding='utf_8_sig')
        except StopIteration:
            loop = False
            print("Iteration is stopped.")


def toRelationCSV(txt_path,csv_path_edge):
    # 读取文件路径与目的文件路径
    txt_path = txt_path
    csv_path_edge = csv_path_edge

    #首先设置文件头
    with open(csv_path_edge, 'w')  as csvfile_edge:
        header = [':START_ID',':END_ID',':TYPE']
        writer = csv.writer(csvfile_edge)
        writer.writerow(header)
    csvfile_edge.close()

    #读取文件， error_bad_lines可能是跳过报错行,header=None是说明txt文件中没有表头。sep='\s*'表示有多个空格作为间隔符
    #reader = pd.read_table(txt_path, iterator=True, encoding='utf8', error_bad_lines=False)
    reader = pd.read_table(txt_path, header=None, sep='\s*',iterator=True, encoding='utf8', error_bad_lines=False)
    #循环读取
    loop = True
    i = 0
    while loop:
        try:
            #每次读取5000个
            chunk = reader.get_chunk(10000000)
            #print(chunk)
            print(i)
            i=i+1
            chunk[2]="Follow"
            #print(chunk)
            
            new_chunk = chunk.drop_duplicates()

            #mode='a' 追加读取, index=False 舍去行数
            new_chunk.to_csv(csv_path_edge, mode='a', index=False, header=None, encoding='utf_8_sig')
        except StopIteration:
            loop = False
            print("Iteration is stopped.")


def removeDuplicate(csv_path_node, final_csv_path_node):
    # 读取文件路径与目的文件路径
    csv_path_node = csv_path_node
    final_csv_path_node = final_csv_path_node

    #首先为final csv设置文件头
    #with open(final_csv_path_node, 'w')  as csvfile_node:
    #    header = ['userid:ID',':LABEL']
    #    writer = csv.writer(csvfile_node)
    #    writer.writerow(header)
    #csvfile_node.close()

    #读取csv
    csvframe = pd.read_csv(csv_path_node, encoding='utf8')
    final_csv= csvframe.drop_duplicates()
    final_csv.to_csv(final_csv_path_node,header=True, index=False)

def removeDuplicate_limitedMemory(csv_path_node, final_csv_path_node):
    # 读取文件路径与目的文件路径
    csv_path_node = csv_path_node
    final_csv_path_node = final_csv_path_node

    #首先为final csv设置文件头
    with open(final_csv_path_node, 'w')  as csvfile_node:
        header = ['userid:ID',':LABEL']
        writer = csv.writer(csvfile_node)
        writer.writerow(header)
    csvfile_node.close()

    #读取csv
    csvreader = pd.read_csv(csv_path_node, header=None, iterator=True, encoding='utf8', error_bad_lines=False)
    #循环读取
    loop = True
    while loop:
        try:
            #每次读取100000个
            chunk = csvreader.get_chunk(90000000)
            new_chunk = chunk.drop_duplicates()
            print(new_chunk.head())
            print("=====")
            print(new_chunk.describe())
            #mode='a' 追加读取, index=False 舍去行数
            new_chunk.to_csv(final_csv_path_node, mode='a', index=False, header=None, encoding='utf_8_sig')
            #time.sleep(10)
        except StopIteration:
            loop = False
            print("removeDuplicate Iteration is stopped.")

def removeDuplicateEdge_limitedMemory(csv_path_edge, final_csv_path_edge):
    # 读取文件路径与目的文件路径
    csv_path_edge = csv_path_edge
    final_csv_path_edge = final_csv_path_edge

    #首先为final csv设置文件头
    with open(final_csv_path_edge, 'w')  as csvfile_edge:
        header = [':START_ID',':END_ID',':TYPE']
        writer = csv.writer(csvfile_edge)
        writer.writerow(header)
    csvfile_edge.close()

    #读取csv
    csvreader = pd.read_csv(csv_path_edge, header=None, iterator=True, encoding='utf8', error_bad_lines=False)
    #循环读取
    loop = True
    i = 0
    while loop:
        try:
            #每次读取100000个
            chunk = csvreader.get_chunk(200000000)
            new_chunk = chunk.drop_duplicates()
            print(new_chunk.head())
            print(i)
            i=i+1
            #print(new_chunk.describe())
            #mode='a' 追加读取, index=False 舍去行数
            new_chunk.to_csv(final_csv_path_edge, mode='a', index=False, header=None, encoding='utf_8_sig')
            #time.sleep(10)
            if i==1: 
                print("end")
                break
        except StopIteration:
            loop = False
            print("removeDuplicateEdge Iteration is stopped.")
    




if __name__ == '__main__':
    txt_path = 'H:/Data/links-anon.txt'

    csv_path_node= 'H:/Data/node.csv'
    final_csv_path_node = "H:/data/noDuplicateNodes.csv"

    csv_path_edge = "H:/data/Relation.csv"


    final_csv_path_node2 = "H:/data/noDuplicateNodes2.csv"
    final_csv_path_node3 = "H:/data/noDuplicateNodes3.csv"
    final_csv_path_node4 = "H:/data/noDuplicateNodes4.csv"
    final_csv_path_node5 = "H:/data/noDuplicateNodes5.csv"
    final_csv_path_node6 = "H:/data/noDuplicateNodes6.csv"

    final_csv_path_edge1 = "H:/data/noDuplicateEdges1.csv"
    final_csv_path_edge2 = "H:/data/noDuplicateEdges2.csv"
    final_csv_path_edge3 = "H:/data/noDuplicateEdges3.csv"
    final_csv_path_edge5 = "H:/data/noDuplicateEdges5.csv"

    #toNodeCSV(txt_path,csv_path_node)
    #removeDuplicate_limitedMemory(final_csv_path_node5,final_csv_path_node6)
    #toRelationCSV(txt_path,csv_path_edge)
    removeDuplicateEdge_limitedMemory(final_csv_path_edge3,final_csv_path_edge5)