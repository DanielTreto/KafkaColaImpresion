package cuatrovientos.dam.psp.KafkaColaImpresion;

import java.util.ArrayList;
import java.util.List;

public class GestorImpresoras {

    public static void main(String[] args) {
        System.out.println("===== GESTOR DE IMPRESORAS INICIADO =====");
        
        List<Thread> impresoras = new ArrayList<>();

        impresoras.add(new Thread(new Impresora("BN", "BN_Oficina1")));
        impresoras.add(new Thread(new Impresora("BN", "BN_Pasillo")));
        impresoras.add(new Thread(new Impresora("BN", "BN_RecursosHumanos")));
        
        impresoras.add(new Thread(new Impresora("Color", "Color_Marketing")));
        impresoras.add(new Thread(new Impresora("Color", "Color_Direccion")));

        for (Thread t : impresoras) {
            t.start();
        }
    }
}
