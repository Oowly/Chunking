import pandas as pd
import logging
from typing import List

def make_chunks_recursion(df: pd.DataFrame, chunk_size, buffer=[]) -> List[pd.DataFrame]:
    '''
    Функия make_chunks_recursion берет на вход pandas DataFrame и chunk_size, после этого проверяет chunk_size на числовое значение и df на pandas DataFrame. 
    Далее проверка на use кейсы (пустой df, отрицательный chunk_size). 
    После удаления дубликатов проверка, что длина половины df меньше или равно chunk_size, иначе не получитсы разбить на чанки, один из чанков будет меньше chunk_size.

    Функция рекурсивно разбивает на чанки df по размеру chunk_size до предпоследнего возможного чанка, а оставшаяся часть df идет в последний чанк. 
    '''
    try:
        chunk_size = int(float(chunk_size))
    except ValueError as e:
        logging.error('chunk_size is not a number')
        logging.error(e)
        return []

    if not isinstance(df, pd.DataFrame):
        logging.error('input df is not a pandas dataframe')
        return []

    if len(df) == 0 or chunk_size <= 0:
        return [df]

    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)

    if chunk_size < len(df):
        new_df = df.loc[chunk_size:,:].reset_index(drop=True)
        buffer.append(df.loc[:chunk_size,:])
        return make_chunks_recursion(new_df,chunk_size,buffer=buffer)
    else:
        if len(df) < chunk_size:
            buffer[-1] = pd.concat([buffer[-1], df]).reset_index(drop=True)
        else:
            buffer.append(df)
        return buffer