"""
Models for contract processing.
This module contains the data classes that represent different aspects of a contract:
- Contrato: Main contract class
- CompaniaInfo: Company information
- ProveedoresInfo: Provider information
- Representantes: Legal representatives
- Entidades: Named entities found in the contract
- Multas: Fines and penalties
"""

from typing import Optional
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class Metadata:
    """Represents metadata about the contract document."""
    nombre_documento: Optional[str] = None
    numero_pagina: Optional[int] = None
    anexos_incluidos: Optional[str] = None
    referencia_interna: Optional[str] = None
    contrato_id: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create a Metadata instance from a dictionary."""
        return cls(
            nombre_documento=data.get('nombre_documento'),
            numero_pagina=data.get('numero_pagina'),
            anexos_incluidos=data.get('anexos_incluidos'),
            referencia_interna=data.get('referencia_interna')
        )

@dataclass
class Contrato:
    """Main contract class representing the contract document."""
    tipo_contrato: str
    tipo_servicio: str
    parte_cliente: str
    parte_proveedor: str
    fecha_inicio: str
    fecha_termino: str
    renovacion_automatica: bool
    monto_total: Optional[float] = None
    termino_anticipado_activo: bool = False
    termino_anticipado_plazo_dias: Optional[int] = None
    exclusividad_activo: bool = False
    exclusividad_detalles: Optional[str] = None
    descripcion: Optional[str] = None
    nombre: Optional[str] = None
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create a Contrato instance from a dictionary."""
        contract_data = {
            'tipo_contrato': data.get('tipo_contrato', 'Contrato General'),
            'tipo_servicio': data.get('tipo_servicio', 'Servicios Generales'),
            'parte_cliente': data.get('parte_cliente', ''),
            'parte_proveedor': data.get('parte_proveedor', ''),
            'fecha_inicio': data.get('fecha_inicio', datetime.now().strftime("%Y-%m-%d")),
            'fecha_termino': data.get('fecha_termino'),
            'renovacion_automatica': data.get('renovacion_automatica', False),
            'monto_total': data.get('monto_total'),
            'termino_anticipado_activo': data.get('termino_anticipado_activo', False),
            'termino_anticipado_plazo_dias': data.get('termino_anticipado_plazo_dias'),
            'exclusividad_activo': data.get('exclusividad_activo', False),
            'exclusividad_detalles': data.get('exclusividad_detalles'),
            'descripcion': data.get('descripcion'),
            'nombre': data.get('nombre')
        }
        
        # Set default fecha_termino if not provided
        if not contract_data['fecha_termino']:
            fecha_inicio = datetime.strptime(contract_data['fecha_inicio'], "%Y-%m-%d")
            contract_data['fecha_termino'] = (fecha_inicio + timedelta(days=365)).strftime("%Y-%m-%d")
        
        return cls(**contract_data)

@dataclass
class CompaniaInfo:
    """Represents company information in the contract."""
    nombre: str
    rut: str
    domicilio: str
    contrato_id: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create a CompaniaInfo instance from a dictionary."""
        return cls(
            nombre=data.get('nombre', ''),
            rut=data.get('rut', ''),
            domicilio=data.get('domicilio', '')
        )

@dataclass
class ProveedoresInfo:
    """Represents provider information in the contract."""
    nombre: str
    rut: str
    domicilio: str
    contrato_id: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create a ProveedoresInfo instance from a dictionary."""
        return cls(
            nombre=data.get('nombre', ''),
            rut=data.get('rut', ''),
            domicilio=data.get('domicilio', '')
        )

@dataclass
class Representante:
    """Represents a legal representative."""
    nombre: str
    cedula_identidad: str
    contrato_id: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create a Representante instance from a dictionary."""
        return cls(
            nombre=data.get('nombre', '').strip(),
            cedula_identidad=data.get('cedula_identidad', '').strip()
        )

@dataclass
class Entidades:
    """Represents an entity with type and value."""
    tipo: str
    valor: str
    contrato_id: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create an Entidades instance from a dictionary."""
        return cls(
            tipo=data.get('tipo', ''),
            valor=data.get('valor', '')
        )

@dataclass
class Multa:
    """Represents a fine/penalty in the contract."""
    tipo_incumplimiento: str
    implicancias: Optional[str] = None
    monto_multa_uf: Optional[str] = None
    plazo_constancia: Optional[str] = None
    descripcion_completa: Optional[str] = None
    contrato_id: Optional[int] = None
    
    @classmethod
    def from_dict(cls, data: dict):
        """Create a Multa instance from a dictionary."""
        return cls(
            tipo_incumplimiento=data.get('tipo_incumplimiento'),
            implicancias=data.get('implicancias'),
            monto_multa_uf=data.get('monto_multa_uf'),
            plazo_constancia=data.get('plazo_constancia'),
            descripcion_completa=data.get('descripcion_completa')
        )

