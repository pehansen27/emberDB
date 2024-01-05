# Peyton Hansen
# DSCI 551 Fall 2023
# Final Project


import os


###################
#database metadata#
###################
database = {}

def showTables(query): #ex// *showTables:table,table,table
    parts = query.split('*')
    parts = deleteSpace(parts)
    showTablesStatement = parts[0].split(':')
    tables = showTablesStatement[1].split(',')
    for table in tables:
        table = str(table)
        table = stripAll(table)
        print(table, database[table])


#########################
#supplementary functions#
#########################
def deleteSpace(list):
    list = list[1:]
    return list


def stripAll(line):
    line = line.replace('[', '').replace(']', '').replace('\'', '').replace('\n','').replace(' ','').replace('\\n','')
    return line


def canConvertToFloat(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def readInChunks(file, chunkSize=100):
    # read a file piece by piece
    while True:
        lines = []
        try:
            for _ in range(chunkSize):
                lines.append(next(file))
        except StopIteration:
            return lines
        yield lines


################
#main functions#
################
def make(query): #ex// *make:tableName-column,column,column
    # decode query
    parts = query.split('*')
    parts1 = deleteSpace(parts)
    parts1 = parts1[0].split(':')
    parts2 = parts1[1].split('-')
    tableName = parts2[0]
    filename = str(tableName) + '.txt'
    # create file
    fileMade = open(filename, 'x')
    columns = parts2[1].split(',')
    table = str(tableName)
    # add table to database dictionary
    database[table] = columns
    return fileMade


def addInto(query): #ex// *addInto:table*values-value,value,value or// *addInto:table*csv:filename
    # decode query
    parts = query.split('*')
    parts = deleteSpace(parts)
    addIntoStatement = parts[0].split(':')
    table = addIntoStatement[1]
    filename = str(table) + '.txt'
    # add row to table
    if 'values' in parts[1]:
        valuesStatement = parts[1].split('-')
        valueList = valuesStatement[1].split(',')
        if len(valueList) == len(database[table]):
            valueString = str(valueList)
            valueString = stripAll(valueString)
            with open(filename, 'a') as file:
                file.write(f"{valueString}\n")
            file.close()
        else:
            print('Invalid number of values inserted.')
    #add full csv into table
    elif 'csv' in parts[1]:
        csvStatement = parts[1].split(':')
        csvFile = csvStatement[1]
        with open(csvFile, 'r') as fileCSV:
            for chunk in readInChunks(fileCSV):
                contents = chunk
                break
            check = contents[0].split(',')
            if len(check) == len(database[table]):
                with open(filename, 'a') as file:
                    with open(csvFile, 'r') as fileCSV:
                        count = 0
                        for chunk in readInChunks(fileCSV):
                            for line in chunk:
                                if count > 0:
                                    file.write(f"{line}")
                                    count+=1
                                else:
                                    count+=1
                        file.write(f"\n")
                        fileCSV.close()
                    file.close()
            else:
                print('Invalid number of values inserted.')


def find(query):
    #ex// *find:column,column*inTable:table*filterBy:column=string
    #or// *find:column,column*inTable:table
    #or// *find:column,column*inTable:table*orderby:column
    #or// *find:column,column*inTable:table*filterBy:column=string*orderby:column
    #or// *find:column*inTable:table*
    parts = query.split('*')
    parts = deleteSpace(parts)
    findStatement = parts[0].split(':')
    columns = findStatement[1].split(',')
    printableColumns = str(columns)
    printableColumns = stripAll(printableColumns)
    print(printableColumns)
    inTableStatement = parts[1].split(':')
    table = inTableStatement[1]
    if len(parts) == 3:
        tableColumns = database[table]
        indices = []
        num = 0
        for value in tableColumns:
            if value in columns:
                indices.append(num)
            num += 1
        thirdStatement = parts[2].split(':')
        if thirdStatement[0] == 'orderby':
            alteredQuery = '*orderBy:' + str(table) + '*column:' + str(thirdStatement[1])
            orderedTable = orderBy(alteredQuery)
            if orderedTable:
                list = []
                chunk_size = 100
                for i in range(0, len(orderedTable), chunk_size):
                    current_chunk = orderedTable[i:i + chunk_size]
                    for line in current_chunk:
                        line = str(line)
                        line = stripAll(line)
                        list.append(line)
                for i in range(0, len(list), chunk_size):
                    current_chunk = list[i:i + chunk_size]
                    for li in current_chunk:
                        li = str(li)
                        li = stripAll(li)
                        printList = []
                        for i in indices:
                            lineList = li.split(',')
                            printList.append(lineList[i])
                        printList = str(printList)
                        printList = stripAll(printList)
                        print(printList)
            else:
                list = []
        else:
            filterByStatement = parts[2].split(':')
            # differentiate between operators
            if '=' in filterByStatement[1]:
                parts2 = filterByStatement[1].split('=')
                attribute = parts2[0]
                comparison = parts2[1]
            elif '<' in filterByStatement[1]:
                parts2 = filterByStatement[1].split('<')
                attribute = parts2[0]
                comparison = parts2[1]
            elif '>' in filterByStatement[1]:
                parts2 = filterByStatement[1].split('>')
                attribute = parts2[0]
                comparison = parts2[1]
            filename = str(table) + '.txt'
            # find indices for columns being found and column being filtered
            tableColumns = database[table]
            # column being filtered
            index = 0
            for value in tableColumns:
                if value == attribute:
                    break
                else:
                    index += 1
            # columns being found
            indices = []
            num = 0
            for value in tableColumns:
                if value in columns:
                    indices.append(num)
                num += 1
            # apply operators
            if '=' in filterByStatement[1]:
                with open(filename, 'r') as file:
                    for chunk in readInChunks(file):
                        for line in chunk:
                            line = line.strip('\n')
                            lineList = line.split(',')
                            if lineList[index] == comparison:
                                valList = []
                                for val in indices:
                                    valList.append(lineList[val])
                                valList = str(valList)
                                valList = stripAll(valList)
                                print(valList)
                    file.close()
            elif '<' in filterByStatement[1]:
                with open(filename, 'r') as file:
                    for chunk in readInChunks(file):
                        if canConvertToFloat(comparison) == True:
                            for line in chunk:
                                line = line.strip('\n')
                                lineList = line.split(',')
                                if float(lineList[index]) < float(comparison):
                                    valList = []
                                    for val in indices:
                                        valList.append(lineList[val])
                                    valList = str(valList)
                                    valList = stripAll(valList)
                                    print(valList)
                        else:
                            print('Cannot compare non-floats with \'<\'')
                    file.close()
            elif '>' in filterByStatement[1]:
                with open(filename, 'r') as file:
                    for chunk in readInChunks(file):
                        if canConvertToFloat(comparison) == True:
                            for line in chunk:
                                line = line.strip('\n')
                                lineList = line.split(',')
                                if float(lineList[index]) > float(comparison):
                                    valList = []
                                    for val in indices:
                                        valList.append(lineList[val])
                                    valList = str(valList)
                                    valList = stripAll(valList)
                                    print(valList)
                        else:
                            print('Cannot compare non-floats with \'>\'')
                    file.close()
    elif len(parts) == 4:
        fourthStatement = parts[3].split(':')
        alteredQuery = '*orderBy:' + str(table) + '*column:' + str(fourthStatement[1])
        orderedTable = orderBy(alteredQuery)
        tableColumns = database[table]
        indices = []
        num = 0
        for value in tableColumns:
            if value in columns:
                indices.append(num)
            num += 1
        if orderedTable:
            orderedList = []
            chunk_size = 100
            for i in range(0, len(orderedTable), chunk_size):
                current_chunk = orderedTable[i:i + chunk_size]
                for line in current_chunk:
                    line = str(line)
                    line = stripAll(line)
                    orderedList.append(line)
            filterByStatement = parts[2].split(':')
            # differentiate between operators
            if '=' in filterByStatement[1]:
                parts2 = filterByStatement[1].split('=')
                attribute = parts2[0]
                comparison = parts2[1]
                index = 0
                for col in tableColumns:
                    if col == attribute:
                        break
                    else:
                        index += 1
                for i in range(0, len(orderedList), chunk_size):
                    current_chunk = orderedList[i:i + chunk_size]
                    for line in current_chunk:
                        line = str(line)
                        line = stripAll(line)
                        lineList = line.split(',')
                        if lineList[index] == comparison:
                            valList = []
                            for val in indices:
                                valList.append(lineList[val])
                            valList = str(valList)
                            valList = stripAll(valList)
                            print(valList)
            elif '<' in filterByStatement[1]:
                parts2 = filterByStatement[1].split('<')
                attribute = parts2[0]
                comparison = parts2[1]
                index = 0
                for col in tableColumns:
                    if col == attribute:
                        break
                    else:
                        index += 1
                for i in range(0, len(orderedList), chunk_size):
                    current_chunk = orderedList[i:i + chunk_size]
                    for line in current_chunk:
                        line = str(line)
                        line = stripAll(line)
                        lineList = line.split(',')
                        if canConvertToFloat(comparison) == True:
                            if float(lineList[index]) < float(comparison):
                                valList = []
                                for val in indices:
                                    valList.append(lineList[val])
                                valList = str(valList)
                                valList = stripAll(valList)
                                print(valList)
                        else:
                            print('Cannot compare non-floats with \'<\'')
            elif '>' in filterByStatement[1]:
                parts2 = filterByStatement[1].split('>')
                attribute = parts2[0]
                comparison = parts2[1]
                index = 0
                for col in tableColumns:
                    if col == attribute:
                        break
                    else:
                        index += 1
                for i in range(0, len(orderedList), chunk_size):
                    current_chunk = orderedList[i:i + chunk_size]
                    for line in current_chunk:
                        line = str(line)
                        line = stripAll(line)
                        lineList = line.split(',')
                        if canConvertToFloat(comparison) == True:
                            if float(lineList[index]) > float(comparison):
                                valList = []
                                for val in indices:
                                    valList.append(lineList[val])
                                valList = str(valList)
                                valList = stripAll(valList)
                                print(valList)
                        else:
                            print('Cannot compare non-floats with \'>\'')
        else:
            orderedList = []
    else:
        filename = str(table) + '.txt'
        # find indices for columns being found
        tableColumns = database[table]
        indices = []
        num = 0
        for value in tableColumns:
            if value in columns:
                indices.append(num)
            num += 1
        with open(filename, 'r') as file:
            for chunk in readInChunks(file):
                for line in chunk:
                    line = line.strip('\n')
                    lineList = line.split(',')
                    valList = []
                    for i in indices:
                        valList.append(lineList[i])
                    valList = str(valList)
                    valList = stripAll(valList)
                    print(valList)
            file.close()


def remove(query):
    #ex// *remove:columnName,columnName*inTable:tableName
    # or// *remove:tableName
    # or// *remove:tableName*filterBy:column=string
    parts = query.split('*')
    parts = deleteSpace(parts)
    if 1 == len(parts):
        removeStatement = parts[0].split(':')
        table = removeStatement[1]
        filename = str(table) + '.txt'
        del database[table]
        if os.path.exists(filename):
            os.remove(filename)
    else:
        secondCommand = parts[1].split(':')
        if secondCommand[0] == 'filterby':
            # delete rows from table
            removeStatement = parts[0].split(':')
            table = removeStatement[1]
            filename = str(table) + '.txt'
            filterByStatement = secondCommand[1]
            if '=' in filterByStatement:
                arguments = filterByStatement.split('=')
                column = arguments[0]
                comparison = arguments[1]
                columns = database[table]
                index = 0
                for c in columns:
                    if c == column:
                        break
                    else:
                        index += 1
                with open(filename, 'r+') as file:
                    tempContents = []
                    for chunk in readInChunks(file):
                        iterate = 0
                        indexList = []
                        for line in chunk:
                            line = line.strip('\n')
                            lineList = line.split(',')
                            if lineList[0] != '':
                                if lineList[index] == comparison:
                                    indexList.append(iterate)
                            iterate += 1
                        for i in sorted(indexList, reverse=True):
                            del chunk[i]
                        tempContents.extend(chunk)
                with open(filename, 'w') as file:
                    file.seek(0)
                    file.truncate()
                    chunk_size = 100
                    for i in range(0, len(tempContents), chunk_size):
                        file.writelines(tempContents[i:i + chunk_size])
                file.close()
            elif '<' in filterByStatement:
                arguments = filterByStatement.split('<')
                column = arguments[0]
                comparison = arguments[1]
                if canConvertToFloat(comparison) == True:
                    columns = database[table]
                    index = 0
                    for c in columns:
                        if c == column:
                            break
                        else:
                            index += 1
                    with open(filename, 'r+') as file:
                        tempContents = []
                        for chunk in readInChunks(file):
                            iterate = 0
                            indexList = []
                            for line in chunk:
                                line = line.strip('\n')
                                lineList = line.split(',')
                                if float(lineList[index]) < float(comparison):
                                    indexList.append(iterate)
                                iterate += 1
                            for i in sorted(indexList, reverse=True):
                                del chunk[i]
                            tempContents.extend(chunk)
                    with open(filename, 'w') as file:
                        file.seek(0)
                        file.truncate()
                        chunk_size = 100
                        for i in range(0, len(tempContents), chunk_size):
                            file.writelines(tempContents[i:i + chunk_size])
                    file.close()
                else:
                    print('Cannot compare non-floats with \'<\'')
            elif '>' in filterByStatement:
                arguments = filterByStatement.split('>')
                column = arguments[0]
                comparison = arguments[1]
                if canConvertToFloat(comparison) == True:
                    columns = database[table]
                    index = 0
                    for c in columns:
                        if c == column:
                            break
                        else:
                            index += 1
                    with open(filename, 'r+') as file:
                        tempContents = []
                        for chunk in readInChunks(file):
                            iterate = 0
                            indexList = []
                            for line in chunk:
                                line = line.strip('\n')
                                lineList = line.split(',')
                                if float(lineList[index]) > float(comparison):
                                    indexList.append(iterate)
                                iterate += 1
                            for i in sorted(indexList, reverse=True):
                                del chunk[i]
                            tempContents.extend(chunk)
                    with open(filename, 'w') as file:
                        file.seek(0)
                        file.truncate()
                        chunk_size = 100
                        for i in range(0, len(tempContents), chunk_size):
                            file.writelines(tempContents[i:i + chunk_size])
                    file.close()
                else:
                    print('Cannot compare non-floats with \'>\'')
        else:
            removeStatement = parts[0].split(':')
            columns = removeStatement[1].split(',')
            inTableStatement = parts[1].split(':')
            table = inTableStatement[1]
            filename = str(table) + '.txt'
            # find indices for columns being found
            tableColumns = database[table]
            indices = []
            num = 0
            for value in tableColumns:
                if value in columns:
                    indices.append(num)
                num += 1
            # delete values from database
            for value in columns:
                tableColumns.remove(value)
            database[table] = tableColumns
            with open(filename, 'r+') as file:
                newLines = []
                for chunk in readInChunks(file):
                    for line in chunk:
                        lineList = line.split(',')
                        for index in sorted(indices, reverse=True):
                            del lineList[index]
                        lineList = str(lineList)
                        lineList = stripAll(lineList)
                        newLines.append(lineList)
            with open(filename, 'w') as file:
                file.seek(0)
                file.truncate()
                chunk_size = 100
                for i in range(0, len(newLines), chunk_size):
                    file.writelines(f"{line}\n" for line in newLines[i:i + chunk_size])
            file.close()


def replace(query): #ex// *replace:table*set:column=value,column=value*filterBy:column=value
    # for the column being updated, I need to find the index of the column and replace the corresponding value with the new value
    parts = query.split('*')
    parts = deleteSpace(parts)
    replaceStatement = parts[0].split(':')
    table = replaceStatement[1]
    filename = str(table) + '.txt'
    setStatement = parts[1].split(':')
    filterByStatement = parts[2].split(':')
    setList = setStatement[1].split(',')
    newValues = []
    columns = []
    for value in setList:
        value = value.split('=')
        column = value[0]
        newValue = value[1]
        newValues.append(newValue)
        columns.append(column)
    tableColumns = database[table]
    indices = []
    num = 0
    for val in tableColumns:
        for col in columns:
            if val == col:
                indices.append(num)
        num += 1
    # replace values in file
    if '=' in filterByStatement[1]:
        filter = filterByStatement[1].split('=')
        filterColumn = filter[0]
        comparison = filter[1]
        index = 0
        for value in tableColumns:
            if value == filterColumn:
                break
            index += 1
        with open(filename, 'r+') as file:
            tempContents = []
            for chunk in readInChunks(file):
                tempChunk = []
                for line in chunk:
                    lineList = line.split(',')
                    compare = lineList[index]
                    compare = str(compare)
                    compare = stripAll(compare)
                    if compare == comparison:
                        itr = 0
                        for i in indices:
                            lineList[i] = newValues[itr]
                            itr += 1
                    lineList = str(lineList)
                    lineList = stripAll(lineList)
                    tempChunk.append(lineList)
                tempContents.extend(tempChunk)
        with open(filename, 'w') as file:
            file.seek(0)
            file.truncate()
            chunk_size = 100
            for i in range(0, len(tempContents), chunk_size):
                file.writelines(f"{line}\n" for line in tempContents[i:i + chunk_size])
        file.close()
    elif '<' in filterByStatement[1]:
        filter = filterByStatement[1].split('<')
        filterColumn = filter[0]
        comparison = filter[1]
        index = 0
        for value in tableColumns:
            if value == filterColumn:
                break
            index += 1
        if canConvertToFloat(comparison) == True:
            with open(filename, 'r+') as file:
                tempContents = []
                for chunk in readInChunks(file):
                    tempChunk = []
                    for line in chunk:
                        lineList = line.split(',')
                        if float(lineList[index]) < float(comparison):
                            # for i in indices:
                            #     for v in newValues:
                            #         lineList[i] = v
                            itr = 0
                            for i in indices:
                                lineList[i] = newValues[itr]
                                itr += 1
                        lineList = str(lineList)
                        lineList = stripAll(lineList)
                        tempChunk.append(lineList)
                    tempContents.extend(tempChunk)
            with open(filename, 'w') as file:
                file.seek(0)
                file.truncate()
                chunk_size = 100
                for i in range(0, len(tempContents), chunk_size):
                    file.writelines(f"{line}\n" for line in tempContents[i:i + chunk_size])
            file.close()
        else:
            print('Cannot compare non-floats with \'<\'')
    elif '>' in filterByStatement[1]:
        filter = filterByStatement[1].split('>')
        filterColumn = filter[0]
        comparison = filter[1]
        index = 0
        for value in tableColumns:
            if value == filterColumn:
                break
            index += 1
        if canConvertToFloat(comparison) == True:
            with open(filename, 'r+') as file:
                tempContents = []
                for chunk in readInChunks(file):
                    tempChunk = []
                    for line in chunk:
                        lineList = line.split(',')
                        if float(lineList[index]) > float(comparison):
                            # for i in indices:
                            #     for v in newValues:
                            #         lineList[i] = v
                            itr = 0
                            for i in indices:
                                lineList[i] = newValues[itr]
                                itr += 1
                        lineList = str(lineList)
                        lineList = stripAll(lineList)
                        tempChunk.append(lineList)
                    tempContents.extend(tempChunk)
            with open(filename, 'w') as file:
                file.seek(0)
                file.truncate()
                chunk_size = 100
                for i in range(0, len(tempContents), chunk_size):
                    file.writelines(f"{line}\n" for line in tempContents[i:i + chunk_size])
            file.close()
        else:
            print('Cannot compare non-floats with \'>\'')


#need to add into find
def groupBy(query):
    # groupBy:column*inTable:table
    # *groupBy:column*inTable:table*aggregateBy:column*function:cnt
    parts = query.split('*')
    parts = deleteSpace(parts)
    tableStatement = parts[1].split(':')
    table = tableStatement[1]
    filename = str(table) + '.txt'
    groupByStatement = parts[0].split(':')
    column = groupByStatement[1]
    column = str(column)
    column = stripAll(column) # need to change
    index = 0
    tableColumns = database[table]
    for value in tableColumns:
        if value == column:
            break
        index += 1
    if len(parts) == 4:
        aggregateByStatement = parts[2].split(':')
        aggregateCol = aggregateByStatement[1]
        aggregateCol = str(aggregateCol)
        aggregateCol = stripAll(aggregateCol)
        index2 = 0
        for value in tableColumns:
            if value == aggregateCol:
                break
            index2 += 1
        functionStatment = parts[3].split(':')
        function = functionStatment[1]
        if function == 'cnt':
            print(column + ' cnt(' + aggregateCol + ')')
            with open(filename, 'r') as file:
                uniqueValues = {}
                for chunk in readInChunks(file):
                    for line in chunk:
                        lineList = line.split(',')
                        add = lineList[index]
                        add = str(add)
                        add = stripAll(add)
                        if add not in uniqueValues:
                            uniqueValues[add] = 1
                        else:
                            count = int(uniqueValues[add]) + 1
                            uniqueValues[add] = count
                print(uniqueValues)
    else:
        print(column)
        with open(filename, 'r') as file:
            uniqueValues = []
            for chunk in readInChunks(file):
                for line in chunk:
                    line = str(line)
                    line = stripAll(line)
                    lineList = line.split(',')
                    if lineList[index] not in uniqueValues:
                        uniqueValues.append(lineList[index])
                    else:
                        continue
            for value in uniqueValues:
                print(value)


def orderBy(query):  # ex// *orderBy:table*column:column
    parts = query.split('*')
    parts = deleteSpace(parts)
    orderByStatement = parts[0].split(':')
    table = orderByStatement[1]
    filename = str(table) + '.txt'
    columnStatement = parts[1].split(':')
    column = columnStatement[1]
    index = 0
    tableColumns = database[table]
    for value in tableColumns:
        if value == column:
            break
        index += 1
    # sort each chunk individually
    sorted_chunks = []
    with open(filename, 'r') as file:
        for chunk in readInChunks(file):
            sorted_chunk = sorted(chunk, key=lambda line: line.split(',')[index])
            if sorted_chunk:
                sorted_chunks.append(iter(sorted_chunk))
    result = []
    current_heads = [next(chunk, None) for chunk in sorted_chunks]
    # merge sorted chunks
    while any(current_heads):
        # find the smallest head
        smallest = None
        smallest_index = -1
        for i, head in enumerate(current_heads):
            if head is not None:
                if smallest is None or head.split(',')[index] < smallest.split(',')[index]:
                    smallest = head
                    smallest_index = i
        if smallest_index != -1:
            # append the smallest element to the result
            result.append(smallest)
            # move to the next element in the chunk that had the smallest element
            current_heads[smallest_index] = next(sorted_chunks[smallest_index], None)
    return result


def connect(query): #ex// *connect:table1,table2*using:column
    parts = query.split('*')
    parts = deleteSpace(parts)
    connectStatement = parts[0].split(':')
    tables = connectStatement[1].split(',')
    table1 = tables[0]
    table2 = tables[1]
    table1Columns = database[table1]
    table2Columns = database[table2]
    colNames = table1Columns + table2Columns
    colNames = str(colNames)
    colNames = stripAll(colNames)
    print(colNames)
    filename1 = str(table1) + '.txt'
    filename2 = str(table2) + '.txt'
    usingStatement = parts[1].split(':')
    column = usingStatement[1]
    index = 0
    for value in table1Columns:
        if value == column:
            break
        index += 1
    index2 = 0
    for col in table2Columns:
        if col == column:
            break
        index2 += 1
    with open(filename1, 'r') as file1:
        for chunk in readInChunks(file1):
            for line in chunk:
                lineList = line.split(',')
                with open(filename2, 'r') as file2:
                    for chunk2 in readInChunks(file2):
                        for line in chunk2:
                            lineList2 = line.split(',')
                            if lineList[index] == lineList2[index2]:
                                joinedLine = lineList + lineList2
                                joinedLine = str(joinedLine)
                                joinedLine = stripAll(joinedLine)
                                print(joinedLine)
        file2.close()
        file1.close()


if __name__ == '__main__':
    print('Welcome to emberDB!\n'
          '...............／＞　　フ\n'
          '..............| 　_　 _|\n'
          '.............／` ミ＿xノ\n'
          '.........../　　　　 |\n'
          '........../　 ヽ　　 ﾉ\n'
          '.........│　　|　|　|\n'
          '.....／￣|　　 |　|　|\n'
          '.....| (￣ヽ＿_ヽ_)__)\n'
          '.....＼二つ')
    value = True
    while value == True:
        userInput = input('emberDB>> ')
        query = userInput.lower()
        if query == 'exit':
            value = False
        else:
            firstCommand = query.split('*')
            firstCommand = deleteSpace(firstCommand)
            command = firstCommand[0].split(':')
            if command[0] == 'showtables':
                showTables(query)
            elif command[0] == 'find':
                find(query)
            elif command[0] == 'make':
                make(query)
            elif command[0] == 'addinto':
                addInto(query)
            elif command[0] == 'remove':
                remove(query)
            elif command[0] == 'replace':  #ex// *replace:table*set:column=value,column=value*filterBy:column=value
               replace(query)
            elif command[0] == 'groupby': #ex// *groupBy:table*column:column or// *groupBy:table*column:column*function:cnt
               groupBy(query)
            elif command[0] == 'orderby': #ex// *orderBy:table*column:column
                parts = query.split('*')
                parts = deleteSpace(parts)
                orderByStatement = parts[0].split(':')
                table = orderByStatement[1]
                columns = database[table]
                columns = str(columns)
                columns = stripAll(columns)
                print(columns)
                sorted_data = orderBy(query)
                # read in chunks
                chunk_size = 100
                for i in range(0, len(sorted_data), chunk_size):
                    current_chunk = sorted_data[i:i + chunk_size]
                    for line in current_chunk:
                        line = str(line)
                        line = stripAll(line)
                        print(line)
            elif command[0] == 'connect': #ex// *connect:table1,table2*using:column
               connect(query)
            else:
                print('Invalid Query')
                value = True
