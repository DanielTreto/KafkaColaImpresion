package cuatrovientos.dam.psp.KafkaColaImpresion;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TransformadorDocumentos {

    private static final String SERVER = "127.0.0.1:9092";
    private static final String T_INCOMING = "print-jobs-incoming";
    private static final String T_BW = "print-docs-bn";
    private static final String T_COLOR = "print-docs-color";
    private static final String GROUP_ID = "transformador-group"; 
    
    private static final int TIEMPO_SONDEO_MS = 500;
    private static final int RETARDO_BASE_MS = 6000;
    private static final int RETARDO_ALEATORIO_MS = 4000;
    private static final int TAMANO_PAGINA_CARACTERES = 400;

    private static final Random random = new Random();
    private static long tiempoInicio = System.currentTimeMillis();

    public static void main(String[] args) {
        Properties propsConsumidor = new Properties();
        propsConsumidor.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        propsConsumidor.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumidor.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsConsumidor.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        propsConsumidor.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        Properties propsProductor = new Properties();
        propsProductor.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
        propsProductor.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsProductor.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(propsConsumidor);
        KafkaProducer<String, String> productor = new KafkaProducer<>(propsProductor);

        consumidor.subscribe(Collections.singleton(T_INCOMING));
        System.out.println("===== TRANSFORMADOR DE DOCUMENTOS INICIADO =====");

        try {
            while (true) {
                ConsumerRecords<String, String> lote = consumidor.poll(Duration.ofMillis(TIEMPO_SONDEO_MS));
                for (ConsumerRecord<String, String> registro : lote) {
                    try {
                        log("Procesando documento");
                        Thread.sleep(RETARDO_BASE_MS + random.nextInt(RETARDO_ALEATORIO_MS));
                        transformar(registro.value(), productor);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            consumidor.close();
            productor.close();
        }
    }

    private static void transformar(String jsonBruto, KafkaProducer<String, String> productor) {
        TrabajoImpresion trabajo = TrabajoImpresion.fromJson(jsonBruto);
        
        String colaDestino = T_BW;
        if ("Color".equalsIgnoreCase(trabajo.getTipo())) {
            colaDestino = T_COLOR;
        }

        String textoCompleto = trabajo.getDocumento();
        int longitudTotal = textoCompleto.length();
        
        int numPaginas = (longitudTotal / TAMANO_PAGINA_CARACTERES) + ((longitudTotal % TAMANO_PAGINA_CARACTERES == 0) ? 0 : 1);
        if (numPaginas == 0) numPaginas = 1; 
        
        log("Transformando el documento: " + trabajo.getTitulo() + " -> " + numPaginas + " partes -> " + colaDestino);

        for (int i = 0; i < numPaginas; i++) {
            int inicio = i * TAMANO_PAGINA_CARACTERES;
            int fin = inicio + TAMANO_PAGINA_CARACTERES;
            if (fin > longitudTotal) fin = longitudTotal;
            
            String contenidoPagina = textoCompleto.substring(inicio, fin);

            String mensaje = "DOC: " + trabajo.getTitulo() + "\n" +
                         "PAG: " + (i + 1) + " DE " + numPaginas + "\n" +
                         "CONTENIDO:\n" + contenidoPagina;
            
            productor.send(new ProducerRecord<>(colaDestino, trabajo.getTitulo(), mensaje));
        }
    }

    private static void log(String mensaje) {
        long tiempoActivo = System.currentTimeMillis() - tiempoInicio;
        System.out.println("[Transformador] [" + tiempoActivo + "ms] " + mensaje);
    }
}
