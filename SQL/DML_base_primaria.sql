--
-- Base de datos: `ods_bona_health`
--
CREATE SCHEMA IF NOT EXISTS `ods_bona_health` DEFAULT CHARACTER SET utf8 ;
-- drop database ` ods_bona_health`;
-- -----------------------------------------------------
-- Schema ods_bona_health
-- -----------------------------------------------------
USE `ods_bona_health` ;
-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `departamento`
--
CREATE TABLE `departamento` (
  `cod_departamento` int(11) NOT NULL,
  `nombre` varchar(45) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `enfermedad`
--
CREATE TABLE `enfermedad` (
  `nombre` varchar(45) NOT NULL,
  `observacion` varchar(45) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `fallecimiento`
--
CREATE TABLE `fallecimiento` (
  `lugar` varchar(45) NOT NULL,
  `fecha` date NOT NULL,
  `persona` varchar(45) NOT NULL,
  `enfermedad` varchar(45) NOT NULL,
  `asistencia_medica` varchar(2) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `fecha`
--
CREATE TABLE `fecha` (
  `fecha_completa` date NOT NULL,
  `año` int(4) NOT NULL,
  `mes` varchar(45) NOT NULL,
  `dia` int(2) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- --------------------------------------------------------

--
-- Estructura de tabla para la tabla `institucion`
--
CREATE TABLE `institucion` (
  `cod_institucion` varchar(45) NOT NULL,
  `nombre` varchar(60) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------
--
-- Estructura de tabla para la tabla `lugar`
--
CREATE TABLE `lugar` (
  `id_lugar` varchar(45) NOT NULL,
  `tipo_lugar` varchar(45) NOT NULL,
  `departamento` int(11) NOT NULL,
  `municipio` int(11) NOT NULL,
  `institucion` varchar(45) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
- --------------------------------------------------------
--
-- Estructura de tabla para la tabla `municipio`
--

CREATE TABLE `municipio` (
  `cod_municipio` int(11) NOT NULL,
  `cod_departamento` int(11) NOT NULL,
  `nombre` varchar(45) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
-- --------------------------------------------------------
--
-- Estructura de tabla para la tabla `persona`
--
CREATE TABLE `persona` (
  `id_persona` varchar(45) NOT NULL,
  `fecha_nam` date NOT NULL,
  `edad` int(2) NOT NULL,
  `sexo` varchar(45) NOT NULL,
  `seg_social` varchar(45) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
--
-- Índices para tablas volcadas
--

--
-- Indices de la tabla `departamento`
--
ALTER TABLE `departamento`
  ADD PRIMARY KEY (`cod_departamento`);
--
-- Indices de la tabla `enfermedad`
--
ALTER TABLE `enfermedad`
  ADD PRIMARY KEY (`nombre`);
--
-- Indices de la tabla `fallecimiento`
--
ALTER TABLE `fallecimiento`
  ADD PRIMARY KEY (`lugar`,`fecha`,`persona`,`enfermedad`),
  ADD KEY `fk_fallecimiento_lugar1_idx` (`lugar`),
  ADD KEY `fk_fallecimiento_fecha1_idx` (`fecha`),
  ADD KEY `fk_fallecimiento_persona1_idx` (`persona`),
  ADD KEY `fk_fallecimiento_enfermedad1_idx` (`enfermedad`);
--
-- Indices de la tabla `fecha`
--
ALTER TABLE `fecha`
  ADD PRIMARY KEY (`fecha_completa`);
--
-- Indices de la tabla `institucion`
--
ALTER TABLE `institucion`
  ADD PRIMARY KEY (`cod_institucion`);
--
-- Indices de la tabla `lugar`
--
ALTER TABLE `lugar`
  ADD PRIMARY KEY (`id_lugar`),
  ADD KEY `fk_lugar_institucion1_idx` (`institucion`),
  ADD KEY `fk_lugar_municipio1_idx` (`municipio`,`departamento`);
--
-- Indices de la tabla `municipio`
--
ALTER TABLE `municipio`
  ADD PRIMARY KEY (`cod_municipio`,`cod_departamento`),
  ADD KEY `fk_municipio_departamento_idx` (`cod_departamento`);
--
-- Indices de la tabla `persona`
--
ALTER TABLE `persona`
  ADD PRIMARY KEY (`id_persona`);
--
-- Restricciones para tablas volcadas
--
--
-- Filtros para la tabla `fallecimiento`
--
ALTER TABLE `fallecimiento`
  ADD CONSTRAINT `fk_fallecimiento_enfermedad1` FOREIGN KEY (`enfermedad`) REFERENCES `enfermedad` (`nombre`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `fk_fallecimiento_fecha1` FOREIGN KEY (`fecha`) REFERENCES `fecha` (`fecha_completa`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `fk_fallecimiento_lugar1` FOREIGN KEY (`lugar`) REFERENCES `lugar` (`id_lugar`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `fk_fallecimiento_persona1` FOREIGN KEY (`persona`) REFERENCES `persona` (`id_persona`) ON DELETE NO ACTION ON UPDATE NO ACTION;
--
-- Filtros para la tabla `lugar`
--
ALTER TABLE `lugar`
  ADD CONSTRAINT `fk_lugar_institucion1` FOREIGN KEY (`institucion`) REFERENCES `institucion` (`cod_institucion`) ON DELETE NO ACTION ON UPDATE NO ACTION,
  ADD CONSTRAINT `fk_lugar_municipio1` FOREIGN KEY (`municipio`,`departamento`) REFERENCES `municipio` (`cod_municipio`, `cod_departamento`) ON DELETE NO ACTION ON UPDATE NO ACTION;
--
-- Filtros para la tabla `municipio`
--
ALTER TABLE `municipio`
  ADD CONSTRAINT `fk_municipio_departamento` FOREIGN KEY (`cod_departamento`) REFERENCES `departamento` (`cod_departamento`) ON DELETE NO ACTION ON UPDATE NO ACTION;
COMMIT;
