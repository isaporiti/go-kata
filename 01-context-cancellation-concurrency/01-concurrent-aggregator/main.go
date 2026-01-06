package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"golang.org/x/sync/errgroup"
)

func main() {
	profileId := 1

	profiles := NewProfileService()
	profiles.profiles.Store(profileId, "Alice")

	orders := NewOrderService()
	orders.orders.Store(profileId, 5)

	aggregator := NewUserAggregator(
		profiles,
		orders,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aggregation, err := aggregator.Aggregate(ctx, profileId)
	if err != nil {
		slog.Error("could not aggregate user",
			"Id", profileId,
			"Error", err,
		)
	}

	slog.Info("aggregation generated successfully",
		"Aggregation", aggregation,
	)
}

type UserAggregator struct {
	profiles *profileService
	orders   *orderService
}

func NewUserAggregator(profiles *profileService, orders *orderService) *UserAggregator {
	return &UserAggregator{
		profiles,
		orders,
	}
}

func (a *UserAggregator) Aggregate(ctx context.Context, id int) (string, error) {
	var (
		eg      errgroup.Group
		profile profile
		orders  orders
	)
	eg.Go(func() error {
		p, err := a.profiles.GetProfile(ctx, id)
		if err != nil {
			return err
		}

		profile = p
		return nil
	})
	eg.Go(func() error {
		o, err := a.orders.GetOrders(ctx, id)
		if err != nil {
			return err
		}

		orders = o
		return nil
	})
	if err := eg.Wait(); err != nil {
		return "", fmt.Errorf("an error occurred while aggregating user: %s", err)
	}

	return fmt.Sprintf(
		"User: %s | Orders: %d",
		profile.Name,
		orders.Quantity,
	), nil
}

type profileService struct {
	profiles sync.Map
}

func NewProfileService() *profileService {
	return &profileService{
		profiles: sync.Map{},
	}
}

func (p *profileService) GetProfile(ctx context.Context, id int) (profile, error) {
	if load, ok := p.profiles.Load(id); ok {
		if name, ok := load.(string); ok {
			return profile{name}, nil
		}
	}
	return profile{}, errors.New("profile not found")
}

type profile struct{ Name string }

type orderService struct {
	orders sync.Map
}

func NewOrderService() *orderService {
	return &orderService{
		orders: sync.Map{},
	}
}

func (o *orderService) GetOrders(ctx context.Context, profileId int) (orders, error) {
	if load, ok := o.orders.Load(profileId); ok {
		if quantity, ok := load.(int); ok {
			return orders{quantity}, nil
		}
	}
	return orders{}, errors.New("order data not found")
}

type orders struct{ Quantity int }
