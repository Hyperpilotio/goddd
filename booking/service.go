// Package booking provides the use-case of booking a cargo. Used by views
// facing an administrator.
package booking

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"time"

	"github.com/marcusolsson/goddd/cargo"
	"github.com/marcusolsson/goddd/location"
	"github.com/marcusolsson/goddd/routing"
)

// ErrInvalidArgument is returned when one or more arguments are invalid.
var ErrInvalidArgument = errors.New("invalid argument")
var r = rand.New(rand.NewSource(99))

// Service is the interface that provides booking methods.
type Service interface {
	// BookNewCargo registers a new cargo in the tracking system, not yet
	// routed.
	BookNewCargo(origin location.UNLocode, destination location.UNLocode, deadline time.Time) (cargo.TrackingID, error)

	// Deletes existing Cargo
	UnbookCargo(cargo.TrackingID) (*cargo.CargoIcon, error)

	// LoadCargo returns a read model of a cargo.
	LoadCargo(id cargo.TrackingID) (Cargo, error)

	// RequestPossibleRoutesForCargo requests a list of itineraries describing
	// possible routes for this cargo.
	RequestPossibleRoutesForCargo(id cargo.TrackingID) []cargo.Itinerary

	// AssignCargoToRoute assigns a cargo to the route specified by the
	// itinerary.
	AssignCargoToRoute(id cargo.TrackingID, itinerary cargo.Itinerary) error

	// ChangeDestination changes the destination of a cargo.
	ChangeDestination(id cargo.TrackingID, destination location.UNLocode) error

	// Cargos returns a list of all cargos that have been booked.
	Cargos() []Cargo

	// Locations returns a list of registered locations.
	Locations() []Location
}

type service struct {
	cargos         cargo.Repository
	locations      location.Repository
	handlingEvents cargo.HandlingEventRepository
	routingService routing.Service
	icons          []*cargo.CargoIcon
}

func (s *service) AssignCargoToRoute(id cargo.TrackingID, itinerary cargo.Itinerary) error {
	if id == "" || len(itinerary.Legs) == 0 {
		return ErrInvalidArgument
	}

	c, err := s.cargos.Find(id)
	if err != nil {
		fmt.Printf("Unable to find cargo %s in assigning cargo to route, error: \n", id, err.Error())
		return err
	}

	c.AssignToRoute(itinerary)

	return s.cargos.Store(c)
}

func (s *service) BookNewCargo(origin, destination location.UNLocode, deadline time.Time) (cargo.TrackingID, error) {
	if origin == "" || destination == "" || deadline.IsZero() {
		return "", ErrInvalidArgument
	}

	id := cargo.NextTrackingID()
	rs := cargo.RouteSpecification{
		Origin:          origin,
		Destination:     destination,
		ArrivalDeadline: deadline,
	}

	c := cargo.New(id, rs)

	if err := s.cargos.Store(c); err != nil {
		return "", err
	}

	return c.TrackingID, nil
}

func (s *service) pickIcon() *cargo.CargoIcon {
	if len(s.icons) == 0 {
		return nil
	}

	return s.icons[r.Intn(len(s.icons))]
}

func (s *service) UnbookCargo(id cargo.TrackingID) (*cargo.CargoIcon, error) {
	rs := cargo.RouteSpecification{}
	c := cargo.New(id, rs)

	if err := s.cargos.Remove(c); err != nil {
		return nil, err
	}

	return s.pickIcon(), nil
}

func (s *service) LoadCargo(id cargo.TrackingID) (Cargo, error) {
	if id == "" {
		return Cargo{}, ErrInvalidArgument
	}

	c, err := s.cargos.Find(id)
	if err != nil {
		return Cargo{}, err
	}

	return assemble(c, s.handlingEvents), nil
}

func (s *service) ChangeDestination(id cargo.TrackingID, destination location.UNLocode) error {
	if id == "" || destination == "" {
		return ErrInvalidArgument
	}

	c, err := s.cargos.Find(id)
	if err != nil {
		return err
	}

	l, err := s.locations.Find(destination)
	if err != nil {
		return err
	}

	c.SpecifyNewRoute(cargo.RouteSpecification{
		Origin:          c.Origin,
		Destination:     l.UNLocode,
		ArrivalDeadline: c.RouteSpecification.ArrivalDeadline,
	})

	if err := s.cargos.Store(c); err != nil {
		return err
	}

	return nil
}

func (s *service) RequestPossibleRoutesForCargo(id cargo.TrackingID) []cargo.Itinerary {
	if id == "" {
		return nil
	}

	c, err := s.cargos.Find(id)
	if err != nil {
		fmt.Printf("Unable to find cargo %s in RequestPossibleRoutesForCargo, error: %s\n", id, err.Error())
		return []cargo.Itinerary{}
	}

	return s.routingService.FetchRoutesForSpecification(c.RouteSpecification)
}

func (s *service) Cargos() []Cargo {
	var result []Cargo
	for _, c := range s.cargos.FindAll() {
		result = append(result, assemble(c, s.handlingEvents))
	}
	return result
}

func (s *service) Locations() []Location {
	var result []Location
	for _, v := range s.locations.FindAll() {
		result = append(result, Location{
			UNLocode: string(v.UNLocode),
			Name:     v.Name,
		})
	}
	return result
}

// NewService creates a booking service with necessary dependencies.
func NewService(cargos cargo.Repository, locations location.Repository, events cargo.HandlingEventRepository, rs routing.Service) Service {
	icons := []*cargo.CargoIcon{}
	infos, err := ioutil.ReadDir("/booking/icons")
	if err != nil {
		fmt.Printf("Wasn't able to read icons directory: %s\n", err.Error())
	} else {
		for _, info := range infos {
			if strings.HasSuffix(info.Name(), ".jpg") {
				bytes, err := ioutil.ReadFile("/booking/icons/" + info.Name())
				if err != nil {
					fmt.Printf("Wasn't able to read icon file %s: %s\n", info.Name(), err.Error())
				} else {
					icons = append(icons, &cargo.CargoIcon{Data: bytes})
				}
			}
		}
	}

	return &service{
		cargos:         cargos,
		locations:      locations,
		handlingEvents: events,
		routingService: rs,
		icons:          icons,
	}
}

// Location is a read model for booking views.
type Location struct {
	UNLocode string `json:"locode"`
	Name     string `json:"name"`
}

// Cargo is a read model for booking views.
type Cargo struct {
	ArrivalDeadline time.Time   `json:"arrival_deadline"`
	Destination     string      `json:"destination"`
	Legs            []cargo.Leg `json:"legs,omitempty"`
	Misrouted       bool        `json:"misrouted"`
	Origin          string      `json:"origin"`
	Routed          bool        `json:"routed"`
	TrackingID      string      `json:"tracking_id"`
}

func assemble(c *cargo.Cargo, events cargo.HandlingEventRepository) Cargo {
	return Cargo{
		TrackingID:      string(c.TrackingID),
		Origin:          string(c.Origin),
		Destination:     string(c.RouteSpecification.Destination),
		Misrouted:       c.Delivery.RoutingStatus == cargo.Misrouted,
		Routed:          !c.Itinerary.IsEmpty(),
		ArrivalDeadline: c.RouteSpecification.ArrivalDeadline,
		Legs:            c.Itinerary.Legs,
	}
}
