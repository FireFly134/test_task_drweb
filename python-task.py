import sys


class _DeletedMarker:
    """Специальный маркер для пометки удалённых значений в транзакциях."""

    pass


DELETED = _DeletedMarker()


class InMemoryDB:
    def __init__(self) -> None:
        self._layers: list[dict[str, object]] = [dict()]

    def _iter_layers_top_down(self):
        """Итерирует слои сверху вниз (от активного к базовому)."""
        for idx in range(len(self._layers) - 1, -1, -1):
            yield self._layers[idx]

    def _effective_get(self, key: str) -> tuple[bool, str | None]:
        """Возвращает с учётом всех слоёв и удалений."""
        for layer in self._iter_layers_top_down():
            if key in layer:
                value = layer[key]
                if value is DELETED:
                    return False, None
                return True, str(value)
        return False, None

    def set_value(self, key: str, value: str) -> None:
        """Устанавливает значение ключа в верхнем слое."""
        self._layers[-1][key] = value

    def get_value(self, key: str) -> str | None:
        """Читает эффективное значение ключа или None, если ключ удалён/не задан."""
        exists, value = self._effective_get(key)
        return value if exists else None

    def unset_value(self, key: str) -> None:
        """Помечает ключ как удалённый в верхнем слое."""
        self._layers[-1][key] = DELETED

    def begin(self) -> None:
        """Начинает транзакцию: добавляет пустой слой поверх стека."""
        self._layers.append(dict())

    def rollback(self) -> bool:
        """Откатывает верхнюю транзакцию. Возвращает True, если была транзакция."""
        if len(self._layers) == 1:
            return False
        self._layers.pop()
        return True

    def commit(self) -> bool:
        """Сливает верхний слой в нижний. Возвращает True, если была транзакция."""
        if len(self._layers) == 1:
            return False
        top = self._layers[-1]
        below = self._layers[-2]
        for k, v in top.items():
            below[k] = v
        self._layers.pop()
        return True

    def _all_keys(self) -> set[str]:
        """Возвращает множество всех ключей, встречающихся во всех слоях."""
        keys: set[str] = set()
        for layer in self._layers:
            keys.update(layer.keys())
        return keys

    def count_value(self, value: str) -> int:
        """Подсчитывает число ключей, чьё эффективное значение равно value."""
        count = 0
        for key in self._all_keys():
            exists, eff = self._effective_get(key)
            if exists and eff == value:
                count += 1
        return count

    def find_keys_by_value(self, value: str) -> list[str]:
        """Возвращает отсортированный список ключей с эффективным значением value."""
        found: list[str] = []
        for key in self._all_keys():
            exists, eff = self._effective_get(key)
            if exists and eff == value:
                found.append(key)
        found.sort()
        return found


def handle_set(db: InMemoryDB, args: list[str]) -> bool:
    """Команда SET: установить значение ключа."""
    if len(args) != 2:
        return False
    db.set_value(args[0], args[1])
    return True


def handle_get(db: InMemoryDB, args: list[str]) -> bool:
    """Команда GET: вывести значение ключа или NULL."""
    if len(args) != 1:
        return False
    value = db.get_value(args[0])
    print(value if value is not None else "NULL")
    return True


def handle_unset(db: InMemoryDB, args: list[str]) -> bool:
    """Команда UNSET: пометить ключ удалённым в текущем слое."""
    if len(args) != 1:
        return False
    db.unset_value(args[0])
    return True


def handle_counts(db: InMemoryDB, args: list[str]) -> bool:
    """Команда COUNTS: вывести число ключей с заданным значением."""
    if len(args) != 1:
        return False
    print(db.count_value(args[0]))
    return True


def handle_find(db: InMemoryDB, args: list[str]) -> bool:
    """Команда FIND: вывести ключи с заданным значением через пробел."""
    if len(args) != 1:
        return False
    keys = db.find_keys_by_value(args[0])
    print(" ".join(keys))
    return True


def handle_begin(db: InMemoryDB, args: list[str]) -> bool:
    """Команда BEGIN: начать транзакцию."""
    if len(args) != 0:
        return False
    db.begin()
    return True


def handle_rollback(db: InMemoryDB, args: list[str]) -> bool:
    """Команда ROLLBACK: откатить текущую (верхнюю) транзакцию."""
    if len(args) != 0:
        return False
    if not db.rollback():
        print("NO TRANSACTION")
    return True


def handle_commit(db: InMemoryDB, args: list[str]) -> bool:
    """Команда COMMIT: зафиксировать текущую (верхнюю) транзакцию."""
    if len(args) != 0:
        return False
    if not db.commit():
        print("NO TRANSACTION")
    return True


COMMANDS = {
    "SET": handle_set,
    "GET": handle_get,
    "UNSET": handle_unset,
    "COUNTS": handle_counts,
    "FIND": handle_find,
    "BEGIN": handle_begin,
    "ROLLBACK": handle_rollback,
    "COMMIT": handle_commit,
}


def main() -> int:
    """Главный цикл REPL. Читает команды, печатает ответы, возвращает код выхода."""
    db = InMemoryDB()
    while True:
        try:
            line = input("> ").strip()
            if not line:
                continue
            cmd_name, *parts = line.split()
            cmd = cmd_name.upper()

            if cmd == "END":
                return 0

            handler = COMMANDS.get(cmd)
            if not handler or not handler(db, parts):
                print("ERROR")

        except EOFError:
            return 0

        except KeyboardInterrupt:
            return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
