import com.sun.scenario.effect.impl.sw.sse.SSEBlend_SRC_OUTPeer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketEmmiter {

    public static void main(String args[]) throws Exception {

        if(args.length == 2) {

            final int portNumber = Integer.parseInt(args[0]); //9643;
            String pastaEntradaCandidatos = args[1];

            File pEntradaCandidatos = new File(pastaEntradaCandidatos);
            File[] pEstados = pEntradaCandidatos.listFiles();

            BufferedReader br;
            String strLine;
            Socket socket;
            OutputStream os;
            PrintWriter pw;
            String nomeArquivo;
            String tipoArquivo;


            System.out.println("Criando servidor socket emissor na porta " + portNumber);
            ServerSocket serverSocket = new ServerSocket(portNumber);
//            while (true) {
                // Aguarda conexão
                socket = serverSocket.accept();
                System.out.println("Conectado...");

                // Inicializa variáveis para emissão das tuplas
                os = socket.getOutputStream();
                pw = new PrintWriter(os, true);

                // Varre todas as pastas de estados, buscando arquivos de prestação de contas dos candidatos por estado
                System.out.println("Emitindo data de arquivos:");
                for (File pEstado : pEstados) {
                    File[] arquivosPEstado = pEstado.listFiles();
                    for (File arquivo : arquivosPEstado) {

                        // Imprime o caminho para o arquivo de leitura
                        System.out.println(arquivo.getAbsolutePath());
                        // Inicializa buffer de leitura de arquivo
                        br = new BufferedReader(new InputStreamReader(new FileInputStream(arquivo), "UTF-8"));
                        // Descarta o cabeçalho
                        strLine = br.readLine();
                        // Recupera nome do arquivo
                        nomeArquivo = arquivo.getName();

                        // Atribui tipo de arquivo D ou R para Despesa ou Receita respectivamente
                        if (nomeArquivo.equals("DespesasCandidatos.txt"))
                            tipoArquivo = "\"D\";";
                        else
                            tipoArquivo = "\"R\";";

                        // Envia linhas lidas em arquivo
                        while ((strLine = br.readLine()) != null) {

                            // Concatena o tipo do arquivo no inicio da linha
                            pw.println(tipoArquivo + strLine);

                        }

                    }
                }
                Thread.sleep(2000);
                // Comando para terminar conexão
                pw.println("\"CLOSE\";" + "07/08/201516:29:38;10000000008;AC;PMDB;15120;Deputado Estadual;FRANCISCO DAS CHAGAS ROMAO;00969060220;Nao;Nota Fiscal;71;10176548000140;GRAFICA SOL;27/08/2010;8250;Publicidade por materiais impressos;Outros Recursos nao descritos;Cheque;CARTAZ SANDINHO FOLDER");

                // Encerra conexão
                pw.close();
                socket.close();
                System.out.println("Conexão encerrada.");
//            }

        } else {
            System.out.println("ERRO: Para a submissao da topologia é necessario 2 argumentos sendo eles:" +
                    "\n1 - Porta para disponibilizar acesso ao servidor" +
                    "\n2 - Caminho para a pasta candidato contendo todos os candidatos por estado");
        }
    }
}