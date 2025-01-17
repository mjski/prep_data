'''
Purpose: Read in a dataset (CSV, SAS7BDAT or pd.DataFrame) and reformat.

Currently written for CSV, DataFrame, or SAS7BDAT data to come in with options dtype=str, and 
keep_default_na=False

Class was converted into two separate classes where PreppedData is an extension of pandas
'''

import pandas as pd
import numpy as np
import pyreadstat


class DataAccessor:
    def __init__(self, *args, **kwargs):
        '''
        Purpose: Use filepath or pandas DataFrame
                 to use survey data. (e.g. df.prep.get_data(path))
        Input: 
            -keyword args **kwargs = path => filepath string to CSV location
                                    data => input Pandas DataFrame
            -*args/**kwargs = checking input for either a filepath or pandas 
                            DataFrame, also for keyword or non keyword 
                            arguments. If "path" is provided then data is 
                            initialized as a pandas DataFrame.
        '''
        
        
        if "delimiter" in kwargs:
            delimiter = kwargs["delimiter"]
        else:
            delimiter = ","
        if "keep_missing" in kwargs:
            keep_missing = kwargs["keep_missing"]
        else:
            keep_missing = True
        # if "keep_metadata" in kwargs:
        #     keep_metadata = kwargs["keep_metadata"]
        # else:
        #     keep_metadata = False

        if len(args) == 1 and isinstance(args[0], str):
            self.path = args[0]
            self.data = pd.DataFrame(self._read_sas_csv(delimiter=delimiter, keep_missing=keep_missing)) # , keep_metadata=keep_metadata (adding later)
        elif len(args) == 1 and isinstance(args[0], pd.DataFrame):
            self.data = args[0]
            self.data = pd.DataFrame(self.data.map(str))
        elif "path" in kwargs:
            self.path = kwargs["path"]
            self.data = pd.DataFrame(self._read_sas_csv(delimiter=delimiter, keep_missing=keep_missing)) # , keep_metadata=keep_metadata (adding later)
        elif "data" in kwargs:
            self.data = kwargs['data']
            self.data = pd.DataFrame(self.data.map(str))
        else:
            raise AttributeError("Invalid input arguments. Arguments must be a pandas DataFrame or a file path to a CSV file or SAS7BDAT file")
        self.to_dataframe()

    def _read_sas_csv(self, delimiter, keep_missing) -> pd.DataFrame: #, keep_metadata (adding later)
        '''
        Note: -This is a private method. Only accessed by DataAccessor() class
              -If special missing character data is important to retain from 
               the input dataset, then CSV is the best option.
        Purpose: reads in CSV or SAS7BDAT file 
                CSV -
                    - reads all columns in as string types (dtype=str), 
                    - allows empty fields (keep_default_na = False), 
                    - allows user to input delimiter of csv (default = ',')
                SAS7BDAT -
                    - two options either to keep numerical missing values or not ## adding option later ##
                    - returns pandas dataframe and optional metadata
        Input: -self.path(must be a .csv or .sas7bdat file) 
               -delimiter with default to comma if CSV
               -keep_missing for sas7bdat with default to True
               -keep_metadata for sas7bdat with default to False
        Output: pandas DataFrame from the input path
        '''
        if ".sas7bdat" in self.path:
            if keep_missing == True:
                self.data, meta = pyreadstat.read_sas7bdat(self.path, user_missing=True) 
                # if keep_metadata == True:
                #     return pd.DataFrame(self.data), meta
                # else:
                #     return pd.DataFrame(self.data)
            else:
                self.data, meta = pyreadstat.read_sas7bdat(self.path)
                
        elif ".csv" in self.path:
            self.data = pd.read_csv(self.path, dtype=str, na_filter=False, sep=delimiter, skipinitialspace=True, quotechar='"') # , quoting=3 adding encoding and quoting might mess things up. need to double check.
        return pd.DataFrame(self.data)
        # if keep_metadata == True:
        #     return pd.DataFrame(self.data), meta
        # else:
        #     return pd.DataFrame(self.data)
        
    def to_dataframe(self):
        return pd.DataFrame(self.data)

    def __repr__(self):
        '''
        Returns a viewable dataframe when class is called 
        '''
        return self.data.__repr__()



################## pandas Extension - PreppedData class below ################## 

@pd.api.extensions.register_dataframe_accessor("prep")
class PreppedData:
    '''
    Purpose: Initialize "prep" pandas extension. Take SAS exported CVS datasets 
             or SAS7BdAT or pandas DataFrame and reformat for survey usage 
             (e.g. df.prep.method_from_below).
    '''
    def __init__(self, pandas_obj):
        self.data = pandas_obj

    def update_blanks(self, blank_val="-97", blank_additions=[]):
        '''
        Purpose: When dataset needs to keep blank values instead of changing to 
                 ".M" then blanks can be changed to input blank_val
        '''
        blank_list = [" ", "", ".", "nan", "NaN", pd.NA, np.nan]
        if len(blank_additions) > 0:
            blank_list = blank_list + blank_additions
        self.data.replace({b:blank_val for b in blank_list}, inplace=True)
        return self.data

    def update_missing(self, miss_val, log_skip_val, miss_additions=[], logskip_additions=[], blank=False):
        '''
        Purpose: update_missing is supposed to change all possible missing values produced by
                 SAS and change to chosen missing and logical_skip values respectively
        Output: Pandas DataFrame
        Note: originally written for specific name variables. This could probably be changed 
              just checking for "NAME", but this would require finding NAME in some variables 
              that are name variable but not other variables that contain NAME but are not name variables.
        Note:
        # find columns without the word NAME and then change values of 
        # "M" to ".M" so the search for missing values changing these values to 
        # the miss_val wanted in the dataframe. At this time I'm not worried
        # about changing it back.
        #  
        # Expanation: 
        # (Name columns are excluded because initials may be present in column 
        #  and should not be changed to missing values 
        # (e.g. FNAME = M) -- this is a valid name and could stand for an 
        #                     M name like Michael but a respondent did not 
        #                     provide their full name)
        '''
        name_cols = [col for col in self.data.columns if "NAME" in col]
        not_name_cols = self.data.columns.difference(name_cols)
        subset = self.data[not_name_cols].copy()
        subset.replace({"M":".M", "L":".L"}, inplace=True)
        self.data.update(subset)
        
        if blank == True:
            miss_lst = [".M"]
        else:
            miss_lst = [".M", "", " ", "nan", "NaN", pd.NA, np.nan, "."]
        
        if len(miss_additions) > 0:
            miss_lst = miss_lst + miss_additions

        self.data.replace({m: miss_val for m in miss_lst}, inplace=True)

        if any(col in self.data.columns for col in name_cols):
            log_skip = [".L"]
            if len(logskip_additions) > 0:
                log_skip = log_skip + logskip_additions
            
            self.data.replace({l: log_skip_val for l in log_skip}, inplace=True)

        else:
            log_skip = [".L", "L"]
            if len(logskip_additions) > 0:
                log_skip = log_skip + logskip_additions
            
            self.data.replace({l: log_skip_val for l in log_skip}, inplace=True)

        return self.data
    
    def update_other_missing(self, add_miss_val, change_to_val, exclude_cols=[]):
        '''
        Purpose: Added method when new missing value is introduced (e.g. .N) for
                 a limited duration in dataset and doesn't fit the normal .M/.L/. 
        Note:    At this time, only 1:1 changes meaning one input value 
                 (add_miss_val) will change to one output value (change_to_val)
        '''
        if isinstance(exclude_cols, str):
            exclude_cols = [exclude_cols]

        dtype_dict = self.data.dtypes.to_dict()
        name_cols = [col for col in self.data.columns if "NAME" in col]
        if len(exclude_cols) > 0:
            name_cols = name_cols + exclude_cols
            
        not_name_cols = self.data.columns.difference(name_cols)
        subset = self.data[not_name_cols].copy()
        subset.replace({add_miss_val:change_to_val}, inplace=True)
        self.data.update(subset)
        self.data = self.data.astype(dtype_dict)
        return self.data

    def capitalize_cols(self):
        '''
        Purpose: capitalize_cols will capitalize and remove excess spaces for all column 
        names
        Output: Pandas DataFrame
        '''
        self.data.columns = self.data.columns.str.upper()
        return self.data
   
    def fix_index(self): 
        '''
        Purpose: If index is added called "UNNAMED: 0" as the column header
                 this method will remove that column.
        '''
        self.data = self.data.loc[:, ~self.data.columns.str.startswith("unnamed")]
        self.data = self.data.loc[:, ~self.data.columns.str.startswith("UNNAMED")]
        return self.data

    def set_int_object(self):
        '''
        Purpose: Make all columns object type without ".0" at the end of each value
        '''
        subset_num = self.data.select_dtypes(exclude="object").columns
        self.data[subset_num] = self.data[subset_num].astype("object")

        subset_obj = list(self.data.select_dtypes(include="object").columns)
        for var in subset_obj:
            self.data[var] = self.data[var].astype(str).str.replace("[.][0]+$", "", regex=True)

        return self.data

    def set_all_object(self):
        '''
        Purpose: Sets all columns to object type -- keeps same format of original columns
        '''
        subset_num = self.data.select_dtypes(exclude="object").columns
        self.data[subset_num] = self.data[subset_num].astype("object")
        return self.data
    
    def convert_num_dtypes(self, exclude=[], as_int=False):
        '''
        Purpose: Allow pandas to infer which columns can be converted to numeric.
        Inputs:  -exclude -- list of variables that should not be considered for 
                             numeric conversion
                 -as_int  -- if column can be of integer type then make it an integer
        Output:  Pandas DataFrame
        '''
        columns = self.data.columns.tolist()
        if len(exclude) > 0:
            for c in exclude:
                columns.remove(c)
        for col in columns:
            try:
                if as_int == True:
                    self.data[col] = pd.to_numeric(self.data[col], downcast='integer')
                else:
                    self.data[col] = pd.to_numeric(self.data[col])
            except:
                pass
        return self.data

    def full_prep(self, miss_val=".M", log_skip_val=".L", blank=False, blank_val="-97", inpt_type=None):
        '''
        Purpose: Run all methods at one time to prepare the dataset for pandas 
                 processing
        Inputs:  miss_val     - passed as input in update_missing() method. This is 
                                the missing value upon input from converted SAS 
                                dataset (default is ".M")
                 log_skip_val - passed as input in update_missing() method. This 
                                is the logical skip value from converted input 
                                SAS dataset (default is ".L")
                 blank        - attribute is either True or False. 
                                If True, the update_blanks() method is called prior 
                                to update_missing() method. 
                                If False, update_blanks() method is not called.
                 blank_val    - passed as input to update_blanks() method if 
                                blank=True. This will convert the input SAS dataset
                                blank values to desired new value (default = "-97").
                #  ## Adding soon ## 11/19/24 
                #  input_type   - Input options: None/"int_obj"/"obj"
                #                 If "int_obj" is given then set_int_object() 
                #                     method is called prior to missing value changes.
                #                 If "obj" is given then set_all_object() method 
                #                     is called prior to missing value changes.
                #                 If None (default), no changes happen to data 
                #                     column types
        '''
        self.capitalize_cols()
        self.fix_index()
        # if inpt_type.lower() == "int_obj":
        #     self.set_int_object()
        # elif inpt_type.lower() == "obj":
        #     self.set_all_object()
        if blank==True:
            self.update_blanks(blank_val)
            self.update_missing(miss_val, log_skip_val, blank=True)
        else:
            self.update_missing(miss_val, log_skip_val)
        return self.data


    # BELOW is incomplete and has not been tested.
    def build_xport(self, output_name, path="./"):
        '''
        INCOMPLETE -- Not ready for use
        '''
        return pyreadstat.write_xport(str(path) + str(output_name) + ".xpt")
