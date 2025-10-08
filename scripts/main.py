from scrapper import ScrapperFutbolenlatv

if __name__ == '__main__':
  #CALENDARIO
  scrapper = ScrapperFutbolenlatv()
  fechas = scrapper.obtener_fechas()
  for fecha in fechas:
    eventos = scrapper.obtener_partidos(fecha)
    if eventos is not None:
      scrapper.guarda_partidos(eventos, fecha)



