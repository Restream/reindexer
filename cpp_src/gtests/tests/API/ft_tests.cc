#include "ft_api.h"

TEST_F(FTApi, CompositeSelect) {
	Add("An entity is something that exists as itself");
	Add("In law, a legal entity is an entity that is capable of bearing legal rights");
	Add("In politics, entity is used as term for territorial divisions of some countries");
	Add("Юридическое лицо — организация, которая имеет обособленное имущество");
	Add("В законодательстве юридическое лицо является лицом, которое может иметь юридические права");
	Add("Уполномоченным лицом (заявителем) могут быть только физические лица Последствия - результат значительного неприятного события");
	Add("Цены на продукты возрасли в результате последствий засухи Как одно из последствий войны ...");
	Add("Предмет - это любая материальная или неосязаемая, видимая или невидимая вещь.  Тот самый предмет из всех имеющихся улик");
	Add("Какой предмет нам бы приобрести для нашего нового клиента ? Предложение со словом и числом8, написанным слитно со словом.");
	Add("Предложение со спец. символами: /, (, +.");

	PrintQueryResults(default_namespace, SimpleCompositeSelect("*в*"));
}
