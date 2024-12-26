import luigi
import os
import wget
import tarfile
import pandas as pd
import io
import logging
import gzip
import glob
import shutil


class DownloadDataset(luigi.Task):
    dataset_id = luigi.Parameter(default='GSE68849')
    download_dir = luigi.Parameter(default='data')  # Каталог для хранения данных

    def output(self):
        # Определяем путь к загружаемому файлу
        return luigi.LocalTarget(os.path.join(self.download_dir, f"{self.dataset_id}_RAW.tar"))

    def run(self):
        # Создать каталог, если он не существует
        os.makedirs(self.download_dir, exist_ok=True)
        
        # Формируем URL для загрузки
        url = f"ftp://ftp.ncbi.nlm.nih.gov/geo/series/GSE68nnn/{self.dataset_id}/suppl/{self.dataset_id}_RAW.tar"
        
        # Загружаем файл
        print(f"Начинаем загрузку: {url}")
        wget.download(url, out=self.output().path)
        print(f"Загрузка завершена и сохранена в {self.output().path}")


class ExtractAndProcessFiles(luigi.Task):
    dataset_id = luigi.Parameter(default='GSE68849')
    download_dir = luigi.Parameter(default='data')  # Каталог для хранения данных
    output_dir = luigi.Parameter(default='processed_data')  # Каталог для сохранения обработанных данных

    def requires(self):
        return DownloadDataset(self.dataset_id, self.download_dir)

    def output(self):
        # Путь к выходному каталогу, где будут храниться обработанные файлы
        return luigi.LocalTarget(self.output_dir)

    def run(self):
        # Создать каталог для обработанных данных, если он не существует
        os.makedirs(self.output_dir, exist_ok=True)

        # Путь к загруженному TAR-архиву
        tar_path = self.input().path
        logging.info(f"Opening TAR file at {tar_path}")

        # Проверка существования файла
        if not os.path.isfile(tar_path):
            logging.error(f"TAR file does not exist: {tar_path}")
            raise FileNotFoundError(f"TAR file does not exist: {tar_path}")

        # Открыть TAR-архив и извлечение файлов
        with tarfile.open(tar_path, 'r') as tar:
            logging.info(f"Extracting files from {tar_path}")
            for member in tar.getmembers():
                if member.isfile() and member.name.endswith('.txt.gz'):
                    # Создание папки для файла
                    member_name = os.path.splitext(member.name)[0]
                    extraction_dir = os.path.join(self.output_dir, member_name)
                    os.makedirs(extraction_dir, exist_ok=True)

                    logging.info(f"Extracting file {member.name} to {extraction_dir}")
                    # Извлечение и распаковка файлов .gz
                    with tar.extractfile(member) as f:
                        with gzip.GzipFile(fileobj=f) as gz:
                            file_content = gz.read()
                            # Сохранение файла без .gz расширения
                            output_file_path = os.path.join(extraction_dir, member_name)
                            with open(output_file_path, 'wb') as out_file:
                                out_file.write(file_content)

                    logging.info(f"File extracted and saved: {output_file_path}")

                    # Обработка файла для разделения на таблицы
                    self.process_file(output_file_path)

        logging.info("All files extracted and processed.")

    def process_file(self, file_path):
        # Разделение файла на таблицы с использованием предоставленного кода
        dfs = {}
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            for line in f.readlines():
                if line.startswith('['):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)

                    # Начинаем новую запись
                    fio = io.StringIO()
                    write_key = line.strip('[]\n')
                    continue
                if write_key:
                    fio.write(line)
            # Обработаем последний сохранённый ключ
            fio.seek(0)
            if write_key:  # Добавлена проверка, чтобы избежать ошибок
                dfs[write_key] = pd.read_csv(fio, sep='\t')

        # Сохраняем каждую таблицу в отдельный файл
        for key, df in dfs.items():
            # Создание названия файла на основе ключа
            output_file_name = f"{os.path.splitext(os.path.basename(file_path))[0]}_{key}.tsv"
            output_file_path = os.path.join(os.path.dirname(file_path), output_file_name)
            df.to_csv(output_file_path, sep='\t', index=False)
            logging.info(f"Saved processed file: {output_file_path}")
    

class TrimProbesTable(luigi.Task):
    dataset_id = luigi.Parameter(default='GSE68849')
    download_dir = luigi.Parameter(default='data')  # Каталог для хранения данных
    processed_data_dir = luigi.Parameter(default='processed_data')  # Директория с обработанными данными
    output_dir = luigi.Parameter(default='trimmed_probes')  # Каталог для сохранения урезанных данных

    def requires(self):
        return ExtractAndProcessFiles(self.dataset_id)

    def output(self):
        return luigi.LocalTarget(os.path.join(self.output_dir, 'Probes_trimmed.tsv'))

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)

        # Список для хранения путей к найденным файлам *_Probes.tsv
        probes_files = []

        # Обход каталога processed_data_dir для поиска файлов *_Probes.tsv
        for root, dirs, files in os.walk(self.processed_data_dir):
            for file in files:
                if file.endswith('_Probes.tsv'):
                    probes_files.append(os.path.join(root, file))

        if not probes_files:
            logging.error(f"No Probes files found in the directory: {self.processed_data_dir}")
            raise FileNotFoundError(f"No Probes files found in the directory: {self.processed_data_dir}")

        # Список колонок для удаления
        columns_to_remove = [
            'Definition', 
            'Ontology_Component', 
            'Ontology_Process', 
            'Ontology_Function', 
            'Synonyms', 
            'Obsolete_Probe_Id', 
            'Probe_Sequence'
        ]

        # Предполагается, что мы обрабатываем каждый найденный файл
        for probes_file_path in probes_files:
            logging.info(f"Loading Probes file: {probes_file_path}")

            # Загружаем таблицу Probes
            probes_df = pd.read_csv(probes_file_path, sep='\t')

            # Удаляем ненужные колонки
            trimmed_probes_df = probes_df.drop(columns=columns_to_remove, errors='ignore')

            # Формирование имени выходного файла
            trimmed_file_path = os.path.join(os.path.dirname(self.output().path), os.path.basename(probes_file_path).replace('_Probes.tsv', '_trimmed_Probes.tsv'))

            # Сохраняем урезанную таблицу
            # output_file_path = self.output().path
            trimmed_probes_df.to_csv(trimmed_file_path, sep='\t', index=False)
            logging.info(f"Trimmed probes table saved to: {trimmed_file_path}")
            
        # Сигнализируем о завершении работы
        with self.output().open('w') as f:
            f.write("Trimmed Probes processing completed.\n")


class DeleteOriginalData(luigi.Task):  
    dataset_id = luigi.Parameter(default='GSE68849')
    download_dir = luigi.Parameter(default='data')  # Каталог для хранения данных
    processed_data_dir = luigi.Parameter(default='processed_data')  # Директория с обработанными данными
    output_dir = luigi.Parameter(default='trimmed_probes')  # Каталог для сохранения урезанных данных


    def requires(self):
        return TrimProbesTable(self.dataset_id)

    def output(self):
        # Можно использовать метку для подтверждения завершения задачи
        return luigi.LocalTarget(os.path.join(self.processed_data_dir, 'delete_original_data.done'))

    def run(self):
        original_items = []

        for root, dirs, files in os.walk(self.processed_data_dir):
            # Добавляем каталоги в список
            for dir in dirs:
                original_items.append(os.path.join(root, dir))
            # Добавляем файлы в список
            for file in files:
                original_items.append(os.path.join(root, file))

        # Use the luigi-interface to log to console
        logger = logging.getLogger('luigi-interface')
        logger.info(f"Found original files: {original_items}")

        if not original_items:
            logging.warning("No original files found to delete.")
            return  # Завершение, если файлы не найдены
        
        # Удаляем файлы
        for file in original_items:
            if os.path.isfile(file):  # Проверяем, что это файл
                logging.info(f"Preparing to delete file: {file}")
                try:
                    os.remove(file)
                    logging.info(f"File deleted successfully: {file}")
                except Exception as e:
                    logging.error(f"Error deleting file {file}: {e}")

        # После удаления файлов, мы идем снова по дереву каталогов и удаляем пустые каталоги
        for root, dirs, files in os.walk(self.processed_data_dir, topdown=False):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                try:
                    os.rmdir(dir_path)  # Пытаемся удалить каталог
                    logging.info(f"Directory deleted successfully: {dir_path}")
                except OSError as e:
                    # Логируем ошибку, если каталог не был пустым или произошла другая ошибка
                    logging.warning(f"Directory not empty or could not be deleted: {dir_path}, Error: {e}")
        
        # Проверяем, является ли processed_data_dir пустым и удаляем его
        try:
            os.rmdir(self.processed_data_dir)
            logger.info(f"Удален пустой основной каталог: {self.processed_data_dir}")
        except OSError as e:
            logger.warning(f"Не удалось удалить основной каталог {self.processed_data_dir}: {e}")
   

if __name__ == '__main__':
    luigi.run()