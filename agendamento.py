import pandas as pd
from sqlalchemy import create_engine

# Carregar os arquivos CSV
agendamentos_df = pd.read_csv('C:/Users/desen/Desktop/teste/Agendamentos.csv', delimiter=';')
contatos_df = pd.read_csv('C:/Users/desen/Desktop/teste/Contatos.csv', delimiter=';')
enderecos_df = pd.read_csv('C:/Users/desen/Desktop/teste/Enderecos.csv', delimiter=';')
pacientes_df = pd.read_csv('C:/Users/desen/Desktop/teste/Pacientes.csv', delimiter=';')

# Filtrar os status permitidos usando o nome correto da coluna "STATUS_AGENDA"
allowed_status = ['Missed', 'Checkout', 'Canceled', 'Confirmed']
agendamentos_filtered_df = agendamentos_df[agendamentos_df['STATUS_AGENDA'].isin(allowed_status)]

# Extrair a data e a hora do campo "DATA_AGENDA"
agendamentos_filtered_df['DATA'] = pd.to_datetime(agendamentos_filtered_df['DATA_AGENDA'])

# A hora de início é a hora no campo "DATA_AGENDA"
agendamentos_filtered_df['hora_inicio'] = agendamentos_filtered_df['DATA'].dt.time

# A "Hora Final" é calculada somando a duração ao "hora_inicio"
agendamentos_filtered_df['hora_final'] = agendamentos_filtered_df['DATA'] + pd.to_timedelta(agendamentos_filtered_df['DURACAO_AGENDA'], unit='m')
agendamentos_filtered_df['hora_final'] = agendamentos_filtered_df['hora_final'].dt.time

# Selecionar e renomear as colunas necessárias
agendamentos_filtered_df = agendamentos_filtered_df[[
    'ID_PACIENTE', 'DENTISTA', 'DATA', 'hora_inicio', 'hora_final', 'STATUS_AGENDA', 'PROCEDIMENTO'
]]

# Mesclar com a tabela de pacientes para adicionar o nome do paciente
merged_agendamentos_df = agendamentos_filtered_df.merge(
    pacientes_df[['ID_PACIENTE', 'NOME_PACIENTE']],
    on='ID_PACIENTE',
    how='left'
)

# Renomear colunas para a estrutura correta
merged_agendamentos_df.rename(columns={
    'ID_PACIENTE': 'id_paciente',
    'NOME_PACIENTE': 'nome_paciente',
    'DENTISTA': 'dentista',
    'DATA': 'data_agenda',
    'hora_inicio': 'hora_inicio',
    'hora_final': 'hora_final',
    'STATUS_AGENDA': 'status',
    'PROCEDIMENTO': 'procedimento'
}, inplace=True)

# Conexão com banco de dados PostgreSQL (substituir com suas credenciais)
db_connection_string = 'postgresql://postgres:123@localhost:5432/postgres'
engine = create_engine(db_connection_string)

# Exportar a tabela de agendamentos para o PostgreSQL
merged_agendamentos_df.to_sql('agendamentos', engine, if_exists='replace', index=False)

print("Dados de agendamentos exportados com sucesso!")
