import pandas as pd
import logging
from typing import List
import math

def evenly_distributed_chunks(df: pd.DataFrame, chunk_size) -> List[pd.DataFrame]:
    '''
    Функия evenly_distributed_chunks берет на вход pandas DataFrame и chunk_size, 
    после этого проверяет chunk_size на числовое значение и df на pandas DataFrame. 
    Далее проверка на use кейсы (пустой df, отрицательный chunk_size). 
    После удаления дубликатов проверка, что длина половины df меньше или равно chunk_size, иначе не получитсы разбить на чанки, один из чанков будет меньше chunk_size.

    num_chunk_total - безостаточное деление общей длины df на chunk_size - отсюда берётся общее понимание числа чанков на выходе.
    num_chunk_greater - остаток от деления df на chunk_size - отсюда берётся понимание, сколько чанков будет превышать по размеру chunk_size.
    chunk_size_greater - размер чанка больше стандартного (данная функция поддерживает комбинацию 2 размеров чанков максимум).
    Если количество num_chunk_greater превышает общее -> df можно разложить только на чанки большего размера (без заданного chunk_size).
    '''
    try:
        chunk_size = int(float(chunk_size))
    except ValueError as e:
        logging.error('Chunk size is not a number')
        logging.error(e)
        return []

    if not isinstance(df, pd.DataFrame):
        logging.error('Input df is not a pandas DataFrame')
        return []

    if len(df) == 0 or chunk_size <= 0:
        return [df]

    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)

    if math.ceil(len(df) / 2) <= chunk_size:
        return [df]
    
    length = len(df)
    num_chunks_total = length // chunk_size
    num_chunks_greater = length % chunk_size
    chunk_size_greater = chunk_size + math.ceil(num_chunks_greater/num_chunks_total) #for how much to increase chunk size

    if num_chunks_greater >= num_chunks_total:
        chunks =  [df.iloc[cur:cur+chunk_size_greater] for cur in range(0, length, chunk_size_greater)]
    else:
        chunks =  [df.iloc[cur:cur+chunk_size_greater] for cur in range(0, num_chunks_greater*chunk_size_greater, chunk_size_greater)] + [df.iloc[cur:cur+chunk_size] for cur in range(num_chunks_greater*chunk_size_greater, length, chunk_size)]
    return chunks