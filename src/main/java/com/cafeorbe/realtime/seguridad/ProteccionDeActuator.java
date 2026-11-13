package com.cafeorbe.realtime.seguridad;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * El realtime-gateway es público (los navegadores se conectan directo). Sus métricas solo las lee el
 * recolector de observabilidad con la credencial interna; la salud queda abierta para las sondas de Azure.
 */
@Component
public class ProteccionDeActuator extends OncePerRequestFilter {

    private final byte[] credencial;

    public ProteccionDeActuator(@Value("${cafeorbe.seguridad.credencial-interna}") String credencial) {
        this.credencial = credencial.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest peticion) {
        String ruta = peticion.getRequestURI();
        return !ruta.startsWith("/actuator") || ruta.equals("/actuator/health") || ruta.startsWith("/actuator/health/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest peticion, HttpServletResponse respuesta, FilterChain cadena)
            throws ServletException, IOException {
        String recibida = peticion.getHeader("X-Internal-Token");
        if (recibida != null && MessageDigest.isEqual(credencial, recibida.getBytes(StandardCharsets.UTF_8))) {
            cadena.doFilter(peticion, respuesta);
            return;
        }
        respuesta.setStatus(HttpServletResponse.SC_FORBIDDEN);
    }
}
