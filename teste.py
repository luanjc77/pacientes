import pandas as pd
import re
from sqlalchemy import create_engine

# Função para limpar CPF, RG e CEP, mantendo apenas números
def clean_number_field(value):
    return re.sub(r'\D', '', str(value))

# Função para separar o endereço em endereço sem número e número
def split_address(address):
    match = re.match(r'(.+?)\s+(\d+)', str(address))
    if match:
        return match.group(1), match.group(2)
    return address, None

# Carregando os arquivos CSV
agendamentos_df = pd.read_csv('C:/Users/desen/Desktop/teste/Agendamentos.csv', delimiter=';')
contatos_df = pd.read_csv('C:/Users/desen/Desktop/teste/Contatos.csv', delimiter=';')
enderecos_df = pd.read_csv('C:/Users/desen/Desktop/teste/Enderecos.csv', delimiter=';')
pacientes_df = pd.read_csv('C:/Users/desen/Desktop/teste/Pacientes.csv', delimiter=';')

# Limpar CPF e RG no dataframe de pacientes
pacientes_df[['CPF_PACIENTE', 'RG_PACIENTE']] = pacientes_df[['CPF_PACIENTE', 'RG_PACIENTE']].applymap(clean_number_field)

# Selecionar o contato mais recente (celular e fixo) para cada paciente
contatos_df_sorted = contatos_df.sort_values(by='DATA_CADASTRO', ascending=False)

# Obter o contato mais recente de celular e telefone fixo
latest_contacts_df = contatos_df_sorted.groupby(['ID_PACIENTE', 'TIPO_CONTATO']).first().reset_index()
latest_contacts_pivot = latest_contacts_df.pivot(index='ID_PACIENTE', columns='TIPO_CONTATO', values='CONTATO').reset_index()
latest_contacts_pivot.columns = ['ID_PACIENTE', 'Celular', 'Telefone Fixo']

# Capturar contatos mais antigos que não são os mais recentes
# Ou seja, quaisquer outros contatos que o paciente tenha
older_contacts_df = contatos_df[~contatos_df[['ID_PACIENTE', 'CONTATO']].apply(tuple, axis=1).isin(
    latest_contacts_df[['ID_PACIENTE', 'CONTATO']].apply(tuple, axis=1))]

# Agrupar os contatos restantes (mais antigos) em uma lista de "Outros Contatos"
other_contacts_grouped = older_contacts_df.groupby('ID_PACIENTE')['CONTATO'].apply(lambda x: ', '.join(x)).reset_index()

# Selecionar o endereço mais recente para cada paciente
enderecos_df_sorted = enderecos_df.sort_values(by='DATA_CRIACAO', ascending=False)
latest_enderecos_df = enderecos_df_sorted.groupby('ID_PACIENTE').first().reset_index()

# Limpar o CEP e dividir o endereço
latest_enderecos_df['CEP'] = latest_enderecos_df['CEP'].apply(clean_number_field)
latest_enderecos_df[['ENDERECO_CLEAN', 'NUMERO']] = latest_enderecos_df['ENDERECO'].apply(lambda x: pd.Series(split_address(x)))

# Mesclar os dados de pacientes, contatos e endereços
merged_df = pacientes_df.merge(latest_enderecos_df[['ID_PACIENTE', 'ENDERECO_CLEAN', 'NUMERO', 'CEP', 'BAIRRO', 'CIDADE', 'ESTADO']],
                               on='ID_PACIENTE', how='left')
merged_df = merged_df.merge(latest_contacts_pivot, on='ID_PACIENTE', how='left')

# Mesclar os contatos antigos na coluna "Outros Contatos"
merged_df = merged_df.merge(other_contacts_grouped.rename(columns={'CONTATO': 'Outros Contatos'}), on='ID_PACIENTE', how='left')

# Renomear colunas conforme a estrutura desejada
merged_df.rename(columns={
    'ID_PACIENTE': 'id_paciente',
    'NOME_PACIENTE': 'nome_paciente',
    'CPF_PACIENTE': 'cpf',
    'RG_PACIENTE': 'rg',
    'ENDERECO_CLEAN': 'endereco',
    'NUMERO': 'numero',
    'CEP': 'cep',
    'BAIRRO': 'bairro',
    'CIDADE': 'cidade',
    'ESTADO': 'estado',
    'Celular': 'celular',
    'Telefone Fixo': 'telefone_fixo'
}, inplace=True)

# Conexão com banco de dados PostgreSQL (substituir com suas credenciais)
db_connection_string = 'postgresql://postgres:123@localhost:5432/postgres'
engine = create_engine(db_connection_string)

# Exportar a tabela mesclada para o PostgreSQL
merged_df.to_sql('pacientes', engine, if_exists='replace', index=False)

print("Dados exportados com sucesso!")
