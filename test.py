from dataclasses import asdict, dataclass

messages = (
    {
        "cookie": "299a0be4a5a79e6a59fdd251b19d78bb",
        "campId": "foo",
        "isFake": True,
    },
    {
        "cookie": "fa85cca91963d8f301e34247048fca39",
        "campId": "bar",
        "isFake": False,
    },
)


@dataclass
class Statistic:
    campaign: str
    clickFraud: float


stat = Statistic(campaign="test", clickFraud=0.5)
print(asdict(stat))
