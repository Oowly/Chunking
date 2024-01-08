import pandas as pd
from typing import List
import logging
import math

def largest_last_chunk(df: pd.DataFrame, chunk_size) -> List[pd.DataFrame]:
    '''
    Функия largest_last_chunk берет на вход pandas DataFrame и chunk_size, после этого проверяет chunk_size на числовое значение и df на pandas DataFrame. 
    Далее проверка на use кейсы (пустой df, отрицательный chunk_size). 
    После удаления дубликатов проверка, что длина половины df меньше или равно chunk_size, иначе не получитсы разбить на чанки, один из чанков будет меньше chunk_size.

    Функция разбивает на чанки df по размеру chunk_size до предпоследнего возможного чанка, а оставшаяся часть df идет в последний чанк. 
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
    
    chunks = [df.iloc[i:i+chunk_size] for i in range(0, (len(df)//chunk_size-1)*chunk_size, chunk_size)] + [df.iloc[(len(df)//chunk_size-1)*chunk_size:]]
    return chunks