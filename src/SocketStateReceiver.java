import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

class Estado{
    private String sigla;
    private double totalReceita;
    private double totalDespesa;
    private static ArrayList<Estado> estados = new ArrayList<>();
    private Estado estado;

    private Estado(String sigla, double totalReceita, double totalDespesa){
        this.sigla = sigla;
        this.totalReceita = totalReceita;
        this.totalDespesa = totalDespesa;
    }

    public static Estado getEstado(String sigla){
        Estado i = null;
        for(Estado estado : estados)
            if(estado.getSigla().equals(sigla))
                i = estado;

        if(i != null)
            return i;
        else {
            i = new Estado(sigla, 0.0, 0.0);
            estados.add(i);
            return i;
        }
    }

    static ArrayList<Estado> getEstados(){
        return estados;
    }

    public String getSigla() {
        return sigla;
    }

    public void setSigla(String sigla) {
        this.sigla = sigla;
    }

    public double getTotalReceita() {
        return totalReceita;
    }

    public void setTotalReceita(double totalReceita) {
        this.totalReceita = totalReceita;
    }

    public double getTotalDespesa() {
        return totalDespesa;
    }

    public void setTotalDespesa(double totalDespesa) {
        this.totalDespesa = totalDespesa;
    }
}

public class SocketStateReceiver {

    public static void main(String args[]) throws Exception {
        String outputPath;
        String str;
        String tipoArquivo;
        String estadoCandidato;
        String nomeArquivoEscrita;
        String valor;
        BufferedWriter pw = null;
        BufferedReader br;
        ArrayList<Estado> estados;
        String[] splited;
        Estado estado;

        if (args.length == 3) {
            final int portNumber = Integer.parseInt(args[0]); // 9644;
            outputPath = args[1];
            nomeArquivoEscrita = args[2];

            System.out.println("Criando servidor socket escrita de Estados na porta " + portNumber);
            ServerSocket serverSocket = new ServerSocket(portNumber);

//            while (true) {
            // Aguarda conexão
            Socket socket = serverSocket.accept();
            System.out.println("Conectado...");

            // Inicializa variável para leitura de tuplas
            br = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            System.out.println("Recebendo dados...");

            // Le todas as tuplas enviadas armazenando-as em uma tabela hash
            do {


                // Le tupla do buffer
                str = br.readLine();

//                System.out.println(str);

                // Separa os campos da tupla
                if(str != null)
                    splited = str.split("\\t");
                else
                    splited = new String[] {"CLOSE","CLOSE"};

                // Verifica integridade da tupla
                if (splited.length == 3 && !splited[0].equals("CLOSE")) {

                    // Atribui valores para as tuplas
                    tipoArquivo = splited[0];
                    estadoCandidato = splited[1];
                    valor = splited[2];

                    // Pega o estado correspondente da lista
                    estado = Estado.getEstado(estadoCandidato);

                    // Atribui valor correspondente
                    if(tipoArquivo.equals("R"))
                        estado.setTotalReceita(Double.parseDouble(valor));
                    else if (tipoArquivo.equals("D"))
                        estado.setTotalDespesa(Double.parseDouble(valor));
                    else
                        str = "null";

                } else {
                    // Atribui condição de parada por falha na integridade do dado recebido
                    str = "null";
                }
            } while (!str.equals("null"));

            System.out.println("Escrevendo arquivo em:\n" + outputPath);

            ArrayList<Estado> es = Estado.getEstados();
            pw = new BufferedWriter(new FileWriter(outputPath + "/" + nomeArquivoEscrita + ".txt", true));


            pw.write("Receita e Despesa por Estado:");
            pw.newLine();
            pw.newLine();
            pw.flush();

            // Ordena por receita
            es.sort((e1,e2) -> (e1.getTotalReceita() > e2.getTotalReceita()?-1:1));
            for(Estado e : es) {
                pw.write(e.getSigla() + String.format("\tRECEITA=%,.2f\tDESPESA=%,.2f", e.getTotalReceita(), e.getTotalDespesa()) );
                pw.newLine();
                pw.flush();
            }



                // Cria arquivo e adiciona dados computados por candidato
                //pw = new PrintWriter(new FileOutputStream(new File(outputPath + "/" + "ELEICOES 2010" + ".txt"), true));
//                pw.write(cf.toString());
//                pw.newLine();
//                pw.flush();

        // Encerra a conexão
        try {
//                pw.close();
            socket.close();
        } catch (Exception ignored){

        }

        System.out.println("Conexão encerada.");
//            }
        } else{
            System.out.println("ERRO: Para a submissão da topologia é necessario 3 argumentos sendo eles:" +
                    "\n1 - Porta para disponibilizar acesso ao servidor" +
                    "\n2 - Caminho para a pasta onde será escrito o arquivo com os dados processados" +
                    "\n3 - Nome do arquivo que será gerado com o resultado do processamento");
        }

    }
}
