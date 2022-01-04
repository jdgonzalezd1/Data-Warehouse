-- -----------------------------------------------------
-- Schema Bona_Health_DW
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `Bona_Health_DW` DEFAULT CHARACTER SET utf8 ;
-- -----------------------------------------------------
-- Schema ods_bona_health
-- -----------------------------------------------------
USE `Bona_Health_DW` ;
-- -----------------------------------------------------
-- Table `Bona_Health_DW`.`dim_enfermedad`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`dim_enfermedad` (
  `nombre` VARCHAR(45) NOT NULL,
  `descript` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`nombre`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `Bona_Health_DW`.`dim_fecha`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`dim_fecha` (
  `fecha_completa` DATE NOT NULL,
  `anio` INT NOT NULL,
  `mes` VARCHAR(45) NOT NULL,
  `dia` INT(2) NOT NULL,
  PRIMARY KEY (`fecha_completa`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `Bona_Health_DW`.`fact_defunciones_fecha`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`fact_defunciones_fecha` (
  `enfermedad` VARCHAR(45) NOT NULL,
  `fecha` DATE NOT NULL,
  `cantidad_fallecidos` INT NOT NULL,
  PRIMARY KEY (`enfermedad`, `fecha`),
  INDEX `fk_Defun_Fecha1_idx` (`fecha` ASC) ,
  CONSTRAINT `fk_Defun_Enfermedad1`
    FOREIGN KEY (`enfermedad`)
    REFERENCES `Bona_Health_DW`.`dim_enfermedad` (`nombre`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Defun_Fecha1`
    FOREIGN KEY (`fecha`)
    REFERENCES `Bona_Health_DW`.`dim_fecha` (`fecha_completa`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `Bona_Health_DW`.`dim_municipio`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`dim_municipio` (
  `id_municipio` VARCHAR(45) NOT NULL,
  `departamento` VARCHAR(45) NOT NULL,
  `municipio` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_municipio`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `Bona_Health_DW`.`fact_asis_medica`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`fact_asis_medica` (
  `fecha` DATE NOT NULL,
  `municipio` VARCHAR(45) NOT NULL,
  `nombre` VARCHAR(45) NOT NULL,
  `sum_asistencia_med` INT NOT NULL,
  `sum_sin_asistencia_med` INT NOT NULL,
  PRIMARY KEY (`fecha`, `municipio`),
  INDEX `fk_Asis_med_Fecha1_idx` (`fecha` ASC) ,
  INDEX `fk_fact_asis_medica_dim_municipio1_idx` (`municipio` ASC) ,
  CONSTRAINT `fk_Asis_med_Fecha1`
    FOREIGN KEY (`fecha`)
    REFERENCES `Bona_Health_DW`.`dim_fecha` (`fecha_completa`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_asis_medica_dim_municipio1`
    FOREIGN KEY (`municipio`)
    REFERENCES `Bona_Health_DW`.`dim_municipio` (`id_municipio`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-------------------------------------------------------
-- Table `Bona_Health_DW`.`fact_defun_por_municipio`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`fact_defun_por_municipio` (
  `fecha` DATE NOT NULL,
  `enfermedad` VARCHAR(45) NOT NULL,
  `municipio` VARCHAR(45) NOT NULL,
  `cant_defun` INT NOT NULL,
  PRIMARY KEY (`fecha`, `enfermedad`, `municipio`),
  INDEX `fk_Defun_muni_Enfermedad1_idx` (`enfermedad` ASC) ,
  INDEX `fk_fact_defun_por_lugar_dim_municipio1_idx` (`municipio` ASC) ,
  CONSTRAINT `fk_Defun_muni_Fecha`
    FOREIGN KEY (`fecha`)
    REFERENCES `Bona_Health_DW`.`dim_fecha` (`fecha_completa`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_Defun_muni_Enfermedad1`
    FOREIGN KEY (`enfermedad`)
    REFERENCES `Bona_Health_DW`.`dim_enfermedad` (`nombre`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_defun_por_lugar_dim_municipio1`
    FOREIGN KEY (`municipio`)
    REFERENCES `Bona_Health_DW`.`dim_municipio` (`id_municipio`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `Bona_Health_DW`.`dim_persona`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`dim_persona` (
  `id_persona` VARCHAR(45) NOT NULL,
  `sexo` VARCHAR(45) NOT NULL,
  `fecha_nac` DATE NOT NULL,
  `edad` INT NOT NULL,
  `seguridad_social` VARCHAR(45) NOT NULL,
  `asistencia_medica` VARCHAR(2) NOT NULL,
  PRIMARY KEY (`id_persona`))
ENGINE = InnoDB;

-- -----------------------------------------------------
-- Table `Bona_Health_DW`.`dim_lugar`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`dim_lugar` (
  `id_lugar` VARCHAR(45) NOT NULL,
  `tipo_lugar` VARCHAR(45) NOT NULL,
  `institucion` VARCHAR(45) NULL,
  `municipio` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id_lugar`),
  INDEX `fk_dim_lugar_dim_municipio1_idx` (`municipio` ASC) ,
  CONSTRAINT `fk_dim_lugar_dim_municipio1`
    FOREIGN KEY (`municipio`)
    REFERENCES `Bona_Health_DW`.`dim_municipio` (`id_municipio`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `Bona_Health_DW`.`fact_muerte`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `Bona_Health_DW`.`fact_muerte` (
  `persona` VARCHAR(45) NOT NULL,
  `lugar` VARCHAR(45) NOT NULL,
  `nombre` VARCHAR(45) NOT NULL,
  `fecha_completa` DATE NOT NULL,
  INDEX `fk_fact_muerte_dim_persona1_idx` (`persona` ASC) ,
  INDEX `fk_fact_muerte_dim_lugar1_idx` (`lugar` ASC) ,
  INDEX `fk_fact_muerte_dim_enfermedad1_idx` (`nombre` ASC) ,
  INDEX `fk_fact_muerte_dim_fecha1_idx` (`fecha_completa` ASC) ,
  PRIMARY KEY (`persona`, `lugar`, `nombre`, `fecha_completa`),
  CONSTRAINT `fk_fact_muerte_dim_persona1`
    FOREIGN KEY (`persona`)
    REFERENCES `Bona_Health_DW`.`dim_persona` (`id_persona`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_muerte_dim_lugar1`
    FOREIGN KEY (`lugar`)
    REFERENCES `Bona_Health_DW`.`dim_lugar` (`id_lugar`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_muerte_dim_enfermedad1`
    FOREIGN KEY (`nombre`)
    REFERENCES `Bona_Health_DW`.`dim_enfermedad` (`nombre`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_fact_muerte_dim_fecha1`
    FOREIGN KEY (`fecha_completa`)
    REFERENCES `Bona_Health_DW`.`dim_fecha` (`fecha_completa`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;
