from selenium import webdriver
import time
import pandas as pd
import datetime

inicio = datetime.datetime.today()

driver = webdriver.Chrome()

driver.get('http://vimoblife.megaerp.online/Login.aspx')

login = driver.find_element_by_id('pnlLogin_TxtUsuario_I')
login.send_keys('TIBAIRRU')

senha = driver.find_element_by_id('pnlLogin_TxtSenha_I')
senha.send_keys('123456')

driver.find_element_by_id('pnlLogin_BtnLogin_CD').click()

time.sleep(2)
paginaInterna = driver.find_elements_by_tag_name('frame')[0]
driver.switch_to.frame(paginaInterna)

driver.find_element_by_id('ctl00_MenuSuperior_DXI3_T').click()  
time.sleep(1)
driver.find_element_by_id('ctl00_MenuSuperior_DXI3i2_').click() 
time.sleep(1)
driver.find_element_by_id('ctl00_RoundPanelConteudo_cphConteudo_pnlFiltros_btnPesquisar_CD').click()
time.sleep(5)
driver.find_element_by_id('ctl00_RoundPanelConteudo_cphConteudo_cbpResultado_gdvRelatorio_DXPagerBottom_DDB').click()
time.sleep(1)
driver.find_element_by_id('ctl00_RoundPanelConteudo_cphConteudo_cbpResultado_gdvRelatorio_DXPagerBottom_PSP_DXI5_').click()
time.sleep(25)

# tabelaPropostas = driver.find_elements_by_class_name('dxgvDataRow_DevEx')
# print(len(tabelaPropostas))
dict_list=[]
for i in range(0,2996):
  driver.find_element_by_id('ctl00_RoundPanelConteudo_cphConteudo_cbpResultado_gdvRelatorio_tccell' + str(i) +'_0').click()

  time.sleep(2)

  paginaDados = driver.find_element_by_id('ctl00_RoundPanelConteudo_cphConteudo_popRelatorioProspect_CIF-1')
  driver.switch_to.frame(paginaDados)

  driver.find_element_by_id('radTipoRelatorio_RB1_I_D').click()
  time.sleep(2)
  dict_dados_proposta={
  'nome' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_Nome').text,
  'nomeFantasia' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_Fantasia').text,
  'tipo' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Ch_TipoPessoa').text,
  'Cpf' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_Cpf').text,
  'Rg' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_Rg').text,
  'Data_expedicao' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Dt_ExPrg').text,
  'OrgEmissorRg' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_OrgEmissorRg').text,
  'Pis' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_Pis').text,
  'Email' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_Email').text,
  'Midia' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pMid_In_Codigo').text,
  'Objetivo' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Ch_Objetivo').text,
  'RecebeEmail' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Bo_MalaDireta').text,
  'RecebeCorrespondencia' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Bo_Corresp').text,
  'EstadoCivil' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Ch_EstadoCivil').text,
  'Sexo' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Ch_Sexo').text,
  'Dt_Nascimento' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Dt_Nascimento').text,
  'Reconhecimento_Firma' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_Rec_Firma').text,
  'LocalNascim' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_LocalNascim').text,
  'Nacionalidade' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_Nacionalidade').text,
  'NomePai' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_NomePai').text,
  'NomeMae' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_St_NomeMae').text,
  'NroFilho' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_In_NroFilho').text,
  'RendaFamiliar' : driver.find_element_by_id('cbpRelatorio_pnlDadosPessoaisRel_pPro_Re_RendaFamiliar').text,
  'ModeloCnh' : driver.find_element_by_id('cbpRelatorio_pnlDadosCnh_pPro_Ch_ModeloCnh').text,
  'CategoriaCnh' : driver.find_element_by_id('cbpRelatorio_pnlDadosCnh_pPro_St_CategoriaCnh').text,
  'RegistroCnh' : driver.find_element_by_id('cbpRelatorio_pnlDadosCnh_pPro_St_RegistroCnh').text,
  'Dt_EmissaoCnh' : driver.find_element_by_id('cbpRelatorio_pnlDadosCnh_pPro_Dt_EmissaoCnh').text,
  'Dt_ValidadeCnh' : driver.find_element_by_id('cbpRelatorio_pnlDadosCnh_pPro_Dt_ValidadeCnh').text,
  'Dt_PrimHabCnh' : driver.find_element_by_id('cbpRelatorio_pnlDadosCnh_pPro_Dt_PrimHabCnh').text,
  'Empancipacao_Tabelionato' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoRel_pPro_St_Ema_Tabelionato').text,
  'Empancipacao_Registro' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoRel_pPro_St_Ema_Registro').text,
  'Empancipacao_Livro' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoRel_pPro_St_Ema_Livro').text,
  'Empancipacao_Folha' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoRel_pPro_St_Ema_Folha').text,
  'Dt_Emancipacao' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoRel_pPro_Dt_Emancipacao').text,
  'Estrangeiro_Naturalizado' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroRel_pPro_St_Naturalizado').text,
  'Estrangeiro_Dt_Naturalizado' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroRel_pPro_Dt_Naturalizado').text,
  'Estrangeiro_Tabelionato' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroRel_pPro_St_Nat_Tabelionato').text,
  'Estrangeiro_Registro' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroRel_pPro_St_Nat_Registro').text,
  'Estrangeiro_Livro' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroRel_pPro_St_Nat_Livro').text,
  'Estrangeiro_Folha' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroRel_pPro_St_Nat_Folha').text,
  'Estrangeiro_Rne' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroRel_pPro_St_Rne').text,
  'Estrangeiro_Passaporte' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroRel_pPro_St_Passaporte').text,
  'Empresa' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisRel_pPro_St_Empresa').text,
  'Profissao' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisRel_pCar_In_Codigo').text,
  'CarteiraTrabalho' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisRel_pPro_St_CartTrabalho').text,
  'Dt_Admissao' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisRel_pPro_Dt_Admissao').text,
  'FaixaSalarial' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisRel_pFsa_In_Codigo').text,
  'Salario' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisRel_pPro_Re_Salario').text,
  'Homepage' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisRel_pPro_St_Homepage').text,
  'ConjugeCpf' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjeCpf').text,
  'ConjugeRg' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjeRg').text,
  'Dt_ExpedicaoRGConjuge' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Dt_ExPrgCje').text,
  'OrgEmissorRgCje' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_OrgEmissorRgCje').text,
  'ConjugePis' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjPis').text,
  'ConjugeSexo' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Ch_CjSexo').text,
  'ConjugeNome' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_Conjuge').text,
  'ConjugeNomePai' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjNomePai').text,
  'ConjugeNomeMae' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjNomeMae').text,
  'Dt_NascConjuge' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Dt_NascConjuge').text,
  'ConjugeLocalNascim' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjLocalNascim').text,
  'ConjugeNacionalidade' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjNacionalidade').text,
  'ConjugeEmail' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjeEmail').text,
  'ConjugeMalaDireta' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_CjeMalaDireta').text,
  'RegimeCasamento' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Ch_RegimeCasamento').text,
  'Lei6515' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Ch_Lei6515').text,
  'Dt_Casamento' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Dt_Casamento').text,
  'CasamentoTabelionato' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_Tabelionato').text,
  'CasamentoRegistro' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_Registro').text,
  'CasamentoLivro' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_Livro').text,
  'CasamentoFolha' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_Folha').text,
  'CasamentoPacto_Nupcial' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_Pacto_Nupcial').text,
  'PactoNupcial_Dt_Registro' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Dt_Registro').text,
  'ParticipacaoBens' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Re_PercPart_RegCas').text,
  'Casado_Com' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Ch_Casado_Com').text,
  'Tipo_Assinatura' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Ch_Tipo_Assinatura').text,
  'Registro_Imovel' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_St_Reg_Imovel').text,
  'Dt_Reg_Imovel' : driver.find_element_by_id('cbpRelatorio_pnlConjugeRel_pPro_Dt_Reg_Imovel').text,
  'Ema_CjTabelionato' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoCjeRel_pPro_St_Ema_CjTabelionato').text,
  'Ema_CjRegistro' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoCjeRel_pPro_St_Ema_CjRegistro').text,
  'Ema_CjLivro' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoCjeRel_pPro_St_Ema_CjLivro').text,
  'Ema_CjFolha' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoCjeRel_pPro_St_Ema_CjFolha').text,
  'Dt_CjEmancipacao' : driver.find_element_by_id('cbpRelatorio_pnlEmancipacaoCjeRel_pPro_Dt_CjEmancipacao').text,
  'CjNaturalizado' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroCjeRel_pPro_St_CjNaturalizado').text,
  'Dt_CjNaturalizado' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroCjeRel_pPro_Dt_CjNaturalizado').text,
  'CjNat_Tabelionato' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroCjeRel_pPro_St_CjNat_Tabelionato').text,
  'CjNat_Registro' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroCjeRel_pPro_St_CjNat_Registro').text,
  'CjNat_Livro' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroCjeRel_pPro_St_CjNat_Livro').text,
  'CjNat_Folha' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroCjeRel_pPro_St_CjNat_Folha').text,
  'CjRne' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroCjeRel_pPro_St_CjRne').text,
  'CjPassaporte' : driver.find_element_by_id('cbpRelatorio_pnlEstrangeiroCjeRel_pPro_St_CjPassaporte').text,
  'CjeEmpresa' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisCjeRel_pPro_St_CjeEmpresa').text,
  'In_Profissao' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisCjeRel_pCje_In_Profissao').text,
  'CjCartTrabalho' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisCjeRel_pPro_St_CjCartTrabalho').text,
  'Dt_CjAdmissao' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisCjeRel_pPro_Dt_CjAdmissao').text,
  'CjCodigo' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisCjeRel_pFsa_In_CjCodigo').text,
  'CjSalario' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisCjeRel_pPro_Re_CjSalario').text,
  'CjTelLocTrab' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisCjeRel_pPro_St_CjTelLocTrab').text,
  'CjeHomepage' : driver.find_element_by_id('cbpRelatorio_pnlDadosProfissionaisCjeRel_pPro_St_CjeHomepage').text,
  'Banco' : driver.find_element_by_id('cbpRelatorio_pnlDadosBancariosRel_pBan_In_Numero').text,
  'Agencia' : driver.find_element_by_id('cbpRelatorio_pnlDadosBancariosRel_pPro_St_Agencia').text,
  'ContaCorrente' : driver.find_element_by_id('cbpRelatorio_pnlDadosBancariosRel_pPro_St_ContaCorrente').text,
  'Financiamento' : driver.find_element_by_id('cbpRelatorio_pnlAnaliseCreditoRel_pPro_Re_Financiamento').text,
  'Subsidio' : driver.find_element_by_id('cbpRelatorio_pnlAnaliseCreditoRel_pPro_Re_Subsidio').text,
  'Fgts' : driver.find_element_by_id('cbpRelatorio_pnlAnaliseCreditoRel_pPro_Re_Fgts').text}

  dict_list.append(dict_dados_proposta)
  driver.switch_to.parent_frame()

  botaoFechar = driver.find_element_by_class_name('dxWeb_pcCloseButton_DevEx')
  driver.execute_script("arguments[0].click();", botaoFechar)

  time.sleep(2)


df = pd.DataFrame(dict_list)
df.to_excel('./testePropostas.xlsx')
time_fim = datetime.datetime.today()
print(time_fim-inicio)

driver.close()