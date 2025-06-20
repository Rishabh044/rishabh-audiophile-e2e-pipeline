from pydantic import BaseModel, Field, field_validator
from typing import Optional
import re

class HeadphonesModel(BaseModel):
    rank: Optional[int] = Field(alias="Rank")
    value_rating: Optional[float] = Field(alias="Value Rating")
    model: Optional[str] = Field(alias="Model")
    price_msrp: Optional[float] = Field(alias="Price (MSRP)")
    signature: Optional[str] = Field(alias="Signature")
    comments: Optional[str] = Field(alias="Comments")
    tone_grade: Optional[str] = Field(alias="Tone Grade")
    technical_grade: Optional[str] = Field(alias="Technical Grade")
    setup: Optional[str] = Field(alias="Setup")
    status: Optional[str] = Field(alias="Status")
    ranksort: Optional[float] = Field(alias="Ranksort")
    tonesort: Optional[float] = Field(alias="Tonesort")
    techsort: Optional[float] = Field(alias="Techsort")
    pricesort: Optional[float] = Field(alias="Pricesort")
    
    class Config:
        allow_population_by_field_name = True  # allows using snake_case names for .dict()
    
    @field_validator("rank")
    def validate_rank(cls, value):
        expected_values =  {"S", "S-", "A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D+", "D", "D-", "E", "F"}
        if value is not None and value not in expected_values:
            raise ValueError(f"Recieved Rank:{value}\n Rank should be one of the {sorted(expected_values)}")
        return value
    
    @field_validator("value_rating")
    def validate_rating(cls, value):
        if value is None:
            return value
        if re.fullmatch(r"★+", value):
            raise ValueError("Invalid Rating. Should only contain ★")
        return value
    
    
    