import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

class Candidato{
    private String estado;
    private String nome;
    private Double receita;
    private Double despesa;
    private static Formatter fm = new Formatter();

    public Candidato(String estado, String nome, Double receita, Double despesa){
        this.estado = estado;
        this.nome = nome;
        this.receita = receita;
        this.despesa = despesa;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }


    public Double getReceita() {
        return receita;
    }

    public void setReceita(Double receita) {
        this.receita = receita;
    }

    public Double getDespesa() {
        return despesa;
    }

    public void setDespesa(Double despesa) {
        this.despesa = despesa;
    }

    @Override
    public String toString() {
//        String s = fm.format("%1$s %2$s RECEITA=%,.2f DESPESA=%,.2f\n", estado, nome, receita, despesa).toString();
//        fm.flush();
        return estado + " " + nome + String.format(" \t\tRECEITA=%,.2f\tDESPESA=%,.2f", receita, despesa);//(estado + " " + nome + " RECEITA=%f DESPESA=%f", receita , despesa);
    }
}

public class SocketReceiver {

    public static void main(String args[]) throws Exception {
        String outputPath;
        String str;
        String tipoArquivo;
        String estadoCandidato;
        String nomeCandidato;
        String valor;
        BufferedWriter pw = null;
        BufferedReader br;
        HashMap<String, Candidato> countMap = new HashMap<>();
        Candidato candidato = null;
        String[] splited;
        String nomeArquivoCandidatos;
        String nomeArquivoRanking;
        HashMap<String,ArrayList<Candidato>> ranking = new HashMap<>();
        ArrayList<Candidato> cl;

        if (args.length == 4) {
            final int portNumber = Integer.parseInt(args[0]); // 9644;
            outputPath = args[1];
            nomeArquivoCandidatos = args[2];
            nomeArquivoRanking = args[3];

            System.out.println("Criando servidor socket de escrita de Candidatos na porta " + portNumber);
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

                    //System.out.println(str);

                    // Separa os campos da tupla
                    if(str != null)
                        splited = str.split("\\t");
                    else
                        splited = new String[] {"CLOSE","CLOSE"};

                    // Verifica integridade da tupla
                    if (splited.length == 4 && !splited[0].equals("CLOSE")) {

                        // Atribui valores para as tuplas
                        tipoArquivo = splited[0];
                        estadoCandidato = splited[1];
                        nomeCandidato = splited[2];
                        valor = splited[3];

                        // Verifica se o candidato ja esta armazenado na tabela hash
                        if (!countMap.containsKey(nomeCandidato)) {

                            // Cria novo candidato e adiciona-o na tabela hash
                            if (tipoArquivo.equals("D"))
                                candidato = new Candidato(estadoCandidato, nomeCandidato, 0.0, Double.parseDouble(valor));
                            else if (tipoArquivo.equals("R"))
                                candidato = new Candidato(estadoCandidato, nomeCandidato, Double.parseDouble(valor), 0.0);

                            countMap.put(nomeCandidato, candidato);
                        } else if (!tipoArquivo.equals("CLOSE")) { // Atualiza valor do candidato
                            candidato = countMap.get(nomeCandidato);
                            if (tipoArquivo.equals("D"))
                                candidato.setDespesa(Double.parseDouble(valor));
                            else if (tipoArquivo.equals("R"))
                                candidato.setReceita(Double.parseDouble(valor));

                            countMap.put(nomeCandidato, candidato);
                        } else {
                            // Atribui condição de parada pois a última tupla foi enviada
                            str = "null";
                        }
                    } else {
                        // Atribui condição de parada por falha na integridade do dado recebido
                        str = "null";
                    }
                } while (!str.equals("null"));

                System.out.println("Escrevendo arquivo em:\n" + outputPath);
                // Itera pela tabela Hash e coloca no arquivo todos os candidatos armazenados
                Iterator<java.util.Map.Entry<String, Candidato>> x = countMap.entrySet().iterator();
                while (x.hasNext()) {

                    // Recupera o conteúdo da tabela Hash
                    String[] conteudoHash = x.next().toString().split("=");
                    // Recupera o candidato
                    Candidato cf = countMap.get(conteudoHash[0]);

                    // Verifica se a lista de Estado ja foi criada
                    if (!ranking.containsKey(cf.getEstado())){
                        // Adiciona nova lista a tabela Hash
                        ranking.put(cf.getEstado(),new ArrayList<Candidato>());
                        cl = ranking.get(cf.getEstado());
                        cl.add(cf);
                    } else {
                        cl = ranking.get(cf.getEstado());
                        cl.add(cf);
                    }

                    // Cria arquivo e adiciona dados computados por candidato
                    //pw = new PrintWriter(new FileOutputStream(new File(outputPath + "/" + "ELEICOES 2010" + ".txt"), true));
                    pw = new BufferedWriter(new FileWriter(outputPath + "/" + nomeArquivoCandidatos + ".txt", true));
                    pw.write(cf.toString());
                    pw.newLine();
                    pw.flush();
                }

                pw.close();

            pw = new BufferedWriter(new FileWriter(outputPath + "/" + nomeArquivoRanking + ".txt", true));
            Iterator<java.util.Map.Entry<String, ArrayList<Candidato>>> y = ranking.entrySet().iterator();
            while (y.hasNext()){
                String[] hashContent = y.next().toString().split("=");
                cl = ranking.get(hashContent[0]);

                // Ordena lista de candidatos por valor de gastos por estado
                cl.sort((c1,c2) -> (c1.getDespesa() > c2.getDespesa()?-1:1));
                // Cria arquivo de ranking top 10 despesa candidato por estado

                pw.write("Candidatos que mais gastaram no estado de " + hashContent[0]);
                pw.newLine();
                pw.newLine();
                pw.flush();

                // tratar
                for(int i=0; i < (cl.size() >= 10?10:cl.size()) ;++i) {
                    pw.write(cl.get(i).toString());
                    pw.newLine();
                    pw.flush();
                }
                pw.newLine();
                pw.flush();
            }

                // Encerra a conexão
                try {
                    pw.close();
                    socket.close();
                } catch (Exception ignored){

                }

                System.out.println("Conexão encerada.");
//            }
        } else{
            System.out.println("ERRO: Para a submissão da topologia é necessario 2 argumentos sendo eles:" +
                    "\n1 - Porta para disponibilizar acesso ao servidor" +
                    "\n2 - Caminho para a pasta onde será escrito o arquivo com os dados processados");
        }

    }
}


// Cria pastas de saída por estado
//                caminhoSaida = new File(outputPath+cf.getEstado());
//                if(!caminhoSaida.exists())
//                    caminhoSaida.mkdir();
//                arquivo = new File(outputPath+"/"+"ELEICOES 2010"+".txt");