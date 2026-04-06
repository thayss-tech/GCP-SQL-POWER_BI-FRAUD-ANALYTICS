-- ===============================================================================================
-- PROYECTO: Sistema de Detección de Fraudes Financieros (AML - Anti-Money Laundering)
-- ARCHIVO: 01_analisis_fraude.sql
-- MOTOR: Google BigQuery
-- DESCRIPCIÓN: Pipeline de transformación y Análisis Exploratorio de Datos (EDA) en SQL para 
--              identificar patrones de fraude, cuentas mula y anomalías financieras.
-- ===============================================================================================

-- ==============================================================================
-- FASE 0: PREPARACIÓN DE DATOS (DATA PREP & FEATURE ENGINEERING)
-- ==============================================================================

-- Propósito: Crear una "Vista" limpia y optimizada para conectar directamente a Power BI.
-- Se renombran columnas para mejor legibilidad y se crea una nueva variable categórica ('risk_category')
-- basada en el monto de la transacción para facilitar la segmentación visual.
CREATE OR REPLACE VIEW `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data` AS
SELECT 
    step,
    type AS transaction_type,
    amount,
    nameOrig AS origin_account,
    oldbalanceOrg AS old_balance_orig,
    newbalanceOrig AS new_balance_orig,
    nameDest AS dest_account,
    oldbalanceDest AS old_balance_dest,
    newbalanceDest AS new_balance_dest,
    isFraud AS is_fraud,
    isFlaggedFraud AS is_flagged_fraud,
    -- Feature Engineering: Categorización de riesgo por monto transferido
    CASE 
        WHEN amount > 100000 THEN 'High Value'
        WHEN amount BETWEEN 10000 AND 100000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS risk_category
FROM `thayss-finance-etl.financial_risk_db.raw_fraud_data`;


-- ==============================================================================
-- FASE 1: ANÁLISIS EXPLORATORIO DE DATOS (EDA BÁSICO)
-- ==============================================================================

-- 1. Resumen General del Desbalance de Clases (Class Imbalance)
-- Propósito: Entender la proporción de transacciones legítimas vs fraudulentas y el volumen monetario.
SELECT 
    is_fraud, 
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_amount_transferred
FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
GROUP BY is_fraud;

-- 2. Vectores de Ataque: ¿Qué métodos usan los estafadores?
-- Propósito: Identificar las tipologías de transacción más vulnerables (Ej. TRANSFER y CASH_OUT).
SELECT 
    transaction_type, 
    COUNT(*) AS total_frauds,
    ROUND(AVG(amount), 2) AS avg_fraud_amount
FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
WHERE is_fraud = 1
GROUP BY transaction_type
ORDER BY total_frauds DESC;

-- 3. Análisis de Riesgo por Categoría Creada
-- Propósito: Validar la distribución del fraude basándonos en la columna 'risk_category' creada en la Vista.
SELECT 
    risk_category,
    COUNT(*) as fraud_count,
    ROUND(SUM(amount), 2) as money_lost
FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
WHERE is_fraud = 1
GROUP BY risk_category
ORDER BY money_lost DESC;

-- 4. Top 5 Entidades de Mayor Riesgo (Cuentas Origen)
-- Propósito: Listar las cuentas individuales responsables del mayor volumen de dinero robado.
SELECT 
    origin_account, 
    COUNT(*) AS number_of_fraud_attempts,
    ROUND(SUM(amount), 2) AS total_money_stolen
FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
WHERE is_fraud = 1
GROUP BY origin_account
ORDER BY total_money_stolen DESC
LIMIT 5;


-- ==============================================================================
-- FASE 2: SQL AVANZADO - DETECCIÓN DE ANOMALÍAS TEMPORALES Y ESTADÍSTICAS
-- ==============================================================================

-- 5. Análisis Temporal usando CTEs y Matemáticas
-- Propósito: Convertir la variable 'step' (horas continuas) en un formato de reloj (0-23h) 
-- usando la función MOD() para descubrir a qué hora del día ocurren más ataques.
WITH HourlyData AS (
    SELECT 
        is_fraud,
        amount,
        MOD(step, 24) AS hour_of_day -- Extrae la hora exacta del día
    FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
)
SELECT 
    hour_of_day,
    COUNT(*) AS fraud_attempts,
    ROUND(SUM(amount), 2) AS money_stolen
FROM HourlyData
WHERE is_fraud = 1
GROUP BY hour_of_day
ORDER BY fraud_attempts DESC;

-- 6. Detección de Valores Atípicos (Outliers) mediante WINDOW FUNCTIONS
-- Propósito: Comparar el monto de un fraude individual contra el promedio histórico 
-- de TODAS las transacciones de su mismo tipo usando OVER(PARTITION BY).
WITH TransactionStats AS (
    SELECT 
        transaction_type,
        amount,
        is_fraud,
        AVG(amount) OVER(PARTITION BY transaction_type) AS avg_amount_for_type
    FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
)
SELECT 
    transaction_type,
    amount AS fraudulent_amount,
    ROUND(avg_amount_for_type, 2) AS normal_average_amount,
    ROUND(amount / avg_amount_for_type, 1) AS times_larger_than_average
FROM TransactionStats
WHERE is_fraud = 1 
  AND amount > (avg_amount_for_type * 10) -- Umbral: Fraudes 10 veces más grandes que el promedio normal
ORDER BY times_larger_than_average DESC
LIMIT 10;


-- ==============================================================================
-- FASE 3: SQL EXPERTO - RIESGO FINANCIERO Y ANTI-LAVADO DE DINERO (AML)
-- ==============================================================================

-- 7. Evolución del Impacto Financiero (Running Total / Totales Acumulados)
-- Propósito: Calcular la pérdida acumulada de dinero a través del tiempo para graficar la "curva de daño".
SELECT 
    step AS simulation_hour,
    ROUND(SUM(amount), 2) AS money_stolen_in_hour,
    ROUND(SUM(SUM(amount)) OVER (ORDER BY step), 2) AS cumulative_money_lost
FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
WHERE is_fraud = 1
GROUP BY step
ORDER BY step;

-- 8. Segmentación por Cuartiles (NTILE)
-- Propósito: Dividir automáticamente los fraudes en 4 grupos exactos según su monto. 
-- Esto ayuda a identificar si el riesgo se concentra en la cantidad de ataques o en ataques de alto valor.
WITH RankedFrauds AS (
    SELECT 
        amount,
        transaction_type,
        NTILE(4) OVER(ORDER BY amount DESC) as fraud_quartile 
    FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
    WHERE is_fraud = 1
)
SELECT 
    fraud_quartile,
    COUNT(*) AS number_of_frauds,
    ROUND(MIN(amount), 2) AS min_amount_in_quartile,
    ROUND(MAX(amount), 2) AS max_amount_in_quartile,
    ROUND(SUM(amount), 2) AS total_money_lost
FROM RankedFrauds
GROUP BY fraud_quartile
ORDER BY fraud_quartile;

-- 9. Detección de "Vaciado de Cuentas" (Account Draining)
-- Propósito: Encontrar víctimas a las que los estafadores les robaron casi todo su saldo disponible de un solo golpe.
WITH AccountImpact AS (
    SELECT 
        origin_account,
        transaction_type,
        amount,
        old_balance_orig,
        ROUND(SAFE_DIVIDE(amount, old_balance_orig) * 100, 2) AS percentage_drained
    FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data`
    WHERE is_fraud = 1 AND old_balance_orig > 0
)
SELECT * FROM AccountImpact
WHERE percentage_drained >= 98.00 -- Umbral de riesgo: Pérdida mayor al 98% de los fondos
ORDER BY amount DESC
LIMIT 10;


-- ==============================================================================
-- FASE 4: ARQUITECTURA DE DATOS - RASTREO DE REDES DE LAVADO (SELF-JOIN)
-- ==============================================================================

-- 10. Patrón Maestro "Transfer & Cash Out" (Detección de Cuentas Mula)
-- Propósito: Rastrear el flujo de dinero ilícito. Consiste en cruzar la tabla consigo misma (Self-Join)
-- para encontrar casos donde una víctima envía dinero (TRANSFER) a una cuenta destino, y esa misma 
-- cuenta destino retira el efectivo (CASH_OUT) poco tiempo después.
SELECT 
    t1.step AS attack_hour,
    t1.origin_account AS victim_account,
    t1.amount AS money_stolen,
    t1.dest_account AS money_mule_account, -- Se identifica la cuenta puente (mula)
    t2.transaction_type AS mule_action,
    t2.step AS cash_out_hour,              -- Hora en la que se efectúa el retiro físico
    t2.amount AS money_cashed_out,
    (t1.amount - t2.amount) AS money_left_in_mule_account
FROM `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data` t1
JOIN `thayss-finance-etl.financial_risk_db.vw_powerbi_fraud_data` t2
  ON t1.dest_account = t2.origin_account -- CONDICIÓN CLAVE: El destino del fraude es el origen del retiro
  AND t2.step >= t1.step                 -- El retiro ocurre en la misma hora del ataque o en horas posteriores
WHERE t1.transaction_type = 'TRANSFER'
  AND t2.transaction_type = 'CASH_OUT'
  AND t1.is_fraud = 1
ORDER BY money_stolen DESC
LIMIT 15;

-- FIN DEL SCRIPT ===============================================================================