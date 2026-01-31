package cuatrovientos.dam.psp.KafkaColaImpresion;

import java.util.ArrayList;
import java.util.List;

/**
 * Gestor que lanza y mantiene activos los hilos de las impresoras.
 * Simula el encendido de varias impresoras (B/N y Color) en diferentes departamentos.
 */
public class GestorImpresoras {

    public static void main(String[] args) {
        System.out.println("===== GESTOR DE IMPRESORAS INICIADO =====");
        
        List<Thread> impresoras = new ArrayList<>();

        // Creación de hilos para impresoras en Blanco y Negro
        impresoras.add(new Thread(new Impresora("BN", "BN_Oficina1")));
        impresoras.add(new Thread(new Impresora("BN", "BN_Pasillo")));
        impresoras.add(new Thread(new Impresora("BN", "BN_RecursosHumanos")));
        
        // Creación de hilos para impresoras a Color
        impresoras.add(new Thread(new Impresora("Color", "Color_Marketing")));
        impresoras.add(new Thread(new Impresora("Color", "Color_Direccion")));

        // Inicio de todos los hilos
        for (Thread t : impresoras) {
            t.start();
        }
    }
}
