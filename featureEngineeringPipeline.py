import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.feature_selection import SelectKBest


"""
FeatureEngineeringPipeline: Responsible for transforming the raw data into structured data suitable for model input. The pipeline processes features into three main categories:
- Time-Based Features: Consists of metrics such as packet rate
- Statistical Features: Descriptive statistics like mean, standard deviation, and quartiles for packet sizes or protocols
- Protocol-specific Features: Features specific to network protocols that are relevant for identifying attack signatures
"""

class FeatureEngineeringPipeline:
    def __init__(self) -> None:
        """
        Initialize the class with a set of transformers and loads dataset
        """

        self.transformers = [
            ('scaler', StandardScaler()),
            ('pca', PCA(n_components=0.95)),
            ('feature_selector', SelectKBest(k=20))
        ]
        self.__load_dataset()
    
    def create_features(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw data into features for model training

        :param raw_data: Raw network traffic as a DataFrame
        :return: DataFrame of engineered features
        """

        features = {}

        # Time-Based features
        features.update(self._create_time_features(raw_data))

        # Statistical features
        features.update(self._create_statistical_features(raw_data))

        # Protocol-specific features
        features.update(self._create_protocol_features(raw_data))

        return pd.DataFrame(features)
    
    def _create_time_features(self, data: pd.DataFrame) -> dict:
        """
        Generate features related to packet timing, like packet rate

        :param data: Raw network data
        :return: Dictionary of time-based features
        """
        return {
            'packet_rate': self._calculate_packet_rate(data),
            'burst_rate': self._calculate_burst_rate(data),
            'inter_arrival_time': self._calculate_iat(data)
        }

    def _calculate_packet_rate(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Caluculate Packet rate using existing features spkts, dpkts and dur

        :param data: Raw network data
        :return: Calculated DataFrame of the network traffic
        """
        # Calculate total packets and packet rate
        data['Total_Packets'] = data['Spkts'] + data['Dpkts']
        data['pps'] = data['Total_Packets'] / data['dur']

        return data['pps']
    
    def __load_dataset(self) -> None:
        self.testing_set = pd.read_csv('kaggle/input/UNSW_NB15_testing-set.csv')
        self.training_set = pd.read_csv('kaggle/input/UNSW_NB15_training-set.csv')
        # self.LIST_EVENTS = pd.read_csv('kaggle/input/UNSW-NB15_LIST_EVENTS.csv')
        # self.NB15_1 = pd.read_csv('kaggle/input/UNSW-NB15_1.csv')
        # self.NB15_2 = pd.read_csv('kaggle/input/UNSW-NB15_2.csv')
        # self.NB15_3 = pd.read_csv('kaggle/input/UNSW-NB15_3.csv')
        # self.NB15_4 = pd.read_csv('kaggle/input/UNSW-NB15_4.csv')
        # self.NB15_features = pd.read_csv('kaggle/input/NUSW-NB15_features.csv', encoding='cp1252')

        # Concat the datasets
        # self.NB15_1.columns = self.NB15_features['Name'] 
        # self.NB15_2.columns = self.NB15_features['Name'] 
        # self.NB15_3.columns = self.NB15_features['Name'] 
        # self.NB15_4.columns = self.NB15_features['Name'] 

        # self.train_df = pd.concat([self.NB15_1, self.NB15_2, self.NB15_3, self.NB15_4], ignore_index=True)

        # Shuffle the dataset
        self.train_df = self.train_df.sample(frac=1, random_state=42).reset_index(drop=True)